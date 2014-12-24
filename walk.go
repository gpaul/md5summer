// example application for blog post: http://gpaul.github.io/blog/2014/12/24/clean-shutdown-example/
package main

import (
	"crypto/md5"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

func main() {
	var rootdir string
	flag.StringVar(&rootdir, "dir", ".", "directory to calculate checksums of")
	flag.Parse()

	// expand paths like "." and "./foo" to "/home" and "/home/foo"
	rootdir, err := filepath.Abs(rootdir)
	if err != nil {
		panic(fmt.Errorf("cannot expand '%s' to absolute path: %v", rootdir, err))
	}

	// check that the path exists
	stat, err := os.Stat(rootdir)
	if err != nil {
		panic(fmt.Errorf("cannot stat '%s': %v", rootdir, err))
	}
	if !stat.IsDir() {
		panic(fmt.Errorf("%s is not a directory", rootdir))
	}

	checksums, err := walkPath(rootdir)
	if err != nil {
		panic(fmt.Errorf("could not calculate checksums: %v", err))
	}
	for _, checksum := range checksums {
		fmt.Println(checksum.String())
	}
}

type ctrl struct {
	// used to accumulate our results
	acc *checksums
	// used to report errors
	errs chan error
	// semaphore to throttle the number of concurrent reads
	throttle throttle
	// used to wait for goroutines to exit
	wg *sync.WaitGroup
}

type throttle chan struct{}

func newThrottle(n int) throttle {
	t := make(throttle, n)
	for ii := 0; ii < n; ii++ {
		t.ready()
	}
	return t
}

func (t throttle) wait()  { <-t }
func (t throttle) ready() { t <- struct{}{} }

func walkPath(path string) ([]checksum, error) {
	const numWorkers = 10

	// setup the control structure
	c := ctrl{
		&checksums{},
		make(chan error, 1),
		newThrottle(numWorkers),
		&sync.WaitGroup{},
	}

	// fn is our os.WalkFunc, it will be called for every file and directory.
	// It starts a goroutine for every file that calculates the file's checksum.
	fn := func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			// we don't checksum directories, only files
			return nil
		}
		if err != nil {
			return err
		}
		// have any workers returned errors?
		select {
		case err = <-c.errs:
			// yes, return that error and terminate the walk
			return err
		default:
			// nope, still going strong
		}
		// wait for a worker to exit
		c.throttle.wait()
		c.wg.Add(1)
		go checksumFile(path, c)
		return nil
	}
	err := filepath.Walk(path, fn)
	c.wg.Wait()
	if err != nil {
		return nil, err
	}
	// check if any of the last few calculations failed
	select {
	case err := <-c.errs:
		// yep, we failed before calculating all checksums
		return nil, err
	default:
		// no errors were reported and c.wg.Wait() ensures that
		// all goroutines have stopped running. This means
		// the entire run was successful!
	}
	return c.acc.checksums(), nil
}

func checksumFile(path string, c ctrl) {
	defer c.wg.Done()
	defer c.throttle.ready()
	// open the file
	file, err := os.Open(path)
	if err != nil {
		notifyErr(c, err)
		return
	}
	defer file.Close()

	// checksum its contents
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		notifyErr(c, err)
		return
	}
	c.acc.add(checksum{path, hash.Sum(nil)})
}

func notifyErr(c ctrl, err error) {
	// Notify the producer of the error.
	// If there's already an error on the channel,
	// don't bother adding another one, just exit.
	select {
	case c.errs <- err:
	default:
	}
}

type checksum struct {
	filepath string
	sum      []byte
}

func (c *checksum) String() string {
	return base64.StdEncoding.EncodeToString(c.sum) + " " + c.filepath
}

type checksums struct {
	lk   sync.Mutex
	sums []checksum
}

func (cs *checksums) add(sum checksum) {
	cs.lk.Lock()
	cs.sums = append(cs.sums, sum)
	cs.lk.Unlock()
}

func (cs *checksums) checksums() []checksum {
	sort.Sort(cs)
	return cs.sums
}

func (cs *checksums) Len() int { return len(cs.sums) }
func (cs *checksums) Less(i, j int) bool {
	return cs.sums[i].filepath < cs.sums[j].filepath
}
func (cs *checksums) Swap(i, j int) {
	cs.sums[i], cs.sums[j] = cs.sums[j], cs.sums[i]
}
