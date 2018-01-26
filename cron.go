// This library implements a cron spec parser and runner.  See the README for
// more details.
package cron

import (
	"log"
	"runtime"
	"sort"
	"time"

	"github.com/satori/go.uuid"
)

type JobStatus int

const (
	Error JobStatus = 0 + iota
	Waitting
	Paused
	Running
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries  []*Entry
	stop     chan struct{}
	add      chan *Entry
	del      chan string
	pause    chan string
	resume   chan string
	snapshot chan []*Entry
	req      chan *request
	onFire   func(*Entry)
	running  bool
	ErrorLog *log.Logger
	location *time.Location
}

type request struct {
	id       string
	response chan *Entry
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job to run.
	Job Job

	ID string

	Status JobStatus
}

func (e *Entry) clone() *Entry {
	return &Entry{
		Schedule: e.Schedule,
		Next:     e.Next,
		Prev:     e.Prev,
		Job:      e.Job,
		ID:       e.ID,
		Status:   e.Status,
	}
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, in the Local time zone.
func New() *Cron {
	return NewWithLocation(time.Now().Location())
}

// NewWithLocation returns a new Cron job runner.
func NewWithLocation(location *time.Location) *Cron {
	return &Cron{
		entries:  nil,
		add:      make(chan *Entry),
		del:      make(chan string),
		pause:    make(chan string),
		resume:   make(chan string),
		stop:     make(chan struct{}),
		snapshot: make(chan []*Entry),
		onFire:   nil,
		running:  false,
		ErrorLog: nil,
		location: location,
	}
}

//FuncJob A wrapper that turns a func() into a cron.Job
type FuncJob func()

//Run implement of Job interface
func (f FuncJob) Run() { f() }

//OnFire regist fire event handler
func (c *Cron) OnFire(f func(*Entry)) {
	c.onFire = f
}

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func()) (string, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job) (string, error) {
	idV4 , _:= uuid.NewV4()
	id := idv4.String()
	err := c.AddJobWithID(spec, cmd, id)
	return id, err
}

//AddJobWithID adds a Job to the Cron to be run on the given schedule with id.
func (c *Cron) AddJobWithID(spec string, cmd Job, id string) error {
	schedule, err := Parse(spec)
	if err != nil {
		return err
	}
	c.Schedule(schedule, cmd, id)
	return nil
}

//AddFuncWithID adds a func to the Cron to be run on the given schedule with given id
func (c *Cron) AddFuncWithID(spec string, cmd func(), id string) error {
	return c.AddJobWithID(spec, FuncJob(cmd), id)
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job, id string) {
	entry := &Entry{
		Schedule: schedule,
		Job:      cmd,
		ID:       id,
		Status:   Waitting,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
		return
	}

	c.add <- entry
}

//RemoveJob remove the job with the given id
func (c *Cron) RemoveJob(id string) {
	if !c.running {
		c.removeJob(id)
	} else {
		c.del <- id
	}
}

func (c *Cron) removeJob(id string) {
	w := 0 // write index
	for _, x := range c.entries {
		if id == x.ID {
			continue
		}
		c.entries[w] = x
		w++
	}
	c.entries = c.entries[:w]
}

func (c *Cron) Entry(id string) (*Entry, bool) {
	if c.running {
		e, ok := c.entry(id)
		if ok {
			return e.clone(), true
		}
		return nil, false
	}
	resp := make(chan *Entry)
	c.req <- &request{id, resp}
	e := <-resp
	close(resp)
	return e, e != nil
}

func (c *Cron) entry(id string) (*Entry, bool) {
	for _, x := range c.entries {
		if id == x.ID {
			return x, true
		}
	}
	return nil, false
}

//PauseJob pause job with the given id
func (c *Cron) PauseJob(id string) {
	if !c.running {
		c.setJobStatus(id, Paused)
	} else {
		c.pause <- id
	}
}

//ResumeJob resume the job with the given id
func (c *Cron) ResumeJob(id string) {
	if !c.running {
		c.setJobStatus(id, Waitting)
		return
	}
	c.resume <- id
}

func (c *Cron) setJobStatus(id string, s JobStatus) {
	e, ok := c.entry(id)
	if ok {
		e.Status = s
	}
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Start the cron scheduler in its own go-routine, or no-op if already started.
func (c *Cron) Start() {
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// func (c *Cron) runWithRecovery(j Job) {
// 	defer func() {
// 		if r := recover(); r != nil {
// 			const size = 64 << 10
// 			buf := make([]byte, size)
// 			buf = buf[:runtime.Stack(buf, false)]
// 			c.logf("cron: panic running job: %v\n%s", r, buf)
// 		}
// 	}()
// 	j.Run()
// }

func (c *Cron) runWithRecovery(e *Entry) {
	if e.Status != Waitting {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			c.logf("cron: panic running job: %v\n%s", r, buf)
		}
		e.Status = Waitting
	}()
	e.Status = Running
	if c.onFire != nil {
		c.onFire(e.clone())
	}
	e.Job.Run()
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	now := time.Now().In(c.location)
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var effective time.Time
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			effective = now.AddDate(10, 0, 0)
		} else {
			effective = c.entries[0].Next
		}

		timer := time.NewTimer(effective.Sub(now))
		select {
		case now = <-timer.C:
			// Run every entry whose next time was this effective time.
			for _, e := range c.entries {
				if e.Next != effective {
					break
				}
				e.Prev = e.Next
				e.Next = e.Schedule.Next(now)
				go c.runWithRecovery(e)
			}
			continue

		case newEntry := <-c.add:
			c.entries = append(c.entries, newEntry)
			newEntry.Next = newEntry.Schedule.Next(time.Now().In(c.location))

		case id := <-c.del:
			c.removeJob(id)

		case id := <-c.pause:
			c.setJobStatus(id, Paused)

		case id := <-c.resume:
			c.setJobStatus(id, Waitting)

		case req := <-c.req:
			e, _ := c.entry(req.id)
			req.response <- e.clone()
		case <-c.snapshot:
			c.snapshot <- c.entrySnapshot()

		case <-c.stop:
			timer.Stop()
			return
		}

		// 'now' should be updated after newEntry and snapshot cases.
		now = time.Now().In(c.location)
		timer.Stop()
	}
}

// Logs an error to stderr or to the configured error log
func (c *Cron) logf(format string, args ...interface{}) {
	if c.ErrorLog != nil {
		c.ErrorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
func (c *Cron) Stop() {
	if !c.running {
		return
	}
	c.stop <- struct{}{}
	c.running = false
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, e.clone())
	}
	return entries
}
