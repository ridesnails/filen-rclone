// Package filen provides an interface to Filen cloud storage.
package filen

import (
	"context"
	"errors"
	"fmt"
	"io"
	pathModule "path"
	"strings"
	"time"

	sdk "github.com/FilenCloudDienste/filen-sdk-go/filen"
	"github.com/FilenCloudDienste/filen-sdk-go/filen/types"

	"github.com/google/uuid"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"golang.org/x/sync/errgroup"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "filen",
		Description: "Filen",
		NewFs:       NewFs,
		Options: []fs.Option{
			{
				Name:     "Email",
				Help:     "The email of your Filen account",
				Required: true,
			},
			{
				Name:       "Password",
				Help:       "The password of your Filen account",
				Required:   true,
				IsPassword: true,
				Sensitive:  true,
			},
			{
				Name: "API Key",
				Help: `An API Key for your Filen account 

Get this using the Filen CLI export-api-key command
You can download the Filen CLI from https://github.com/FilenCloudDienste/filen-cli`,
				Required:   true,
				IsPassword: true,
				Sensitive:  true,
			},
			{
				Name:     config.ConfigEncoding,
				Help:     config.ConfigEncodingHelp,
				Advanced: true,
				Default:  encoder.Standard | encoder.EncodeInvalidUtf8,
			}, {
				Name:      "MasterKeys",
				Help:      "Master Keys (internal use only)",
				Sensitive: true,
				Advanced:  true,
			}, {
				Name:      "PrivateKey",
				Help:      "Private RSA Key (internal use only)",
				Sensitive: true,
				Advanced:  true,
			}, {
				Name:      "PublicKey",
				Help:      "Public RSA Key (internal use only)",
				Sensitive: true,
				Advanced:  true,
			}, {
				Name:     "AuthVersion",
				Help:     "Authentication Version (internal use only)",
				Advanced: true,
			}, {
				Name:      "BaseFolderUUID",
				Help:      "UUID of Account Root Directory (internal use only)",
				Sensitive: true,
				Advanced:  true,
			},
		},
	})
}

// NewFs constructs a Fs at the path root
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	password, err := obscure.Reveal(opt.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to reveal password: %w", err)
	}
	apiKey, err := obscure.Reveal(opt.APIKey)
	if err != nil {
		return nil, fmt.Errorf("failed to reveal api key: %w", err)
	}

	var filen *sdk.Filen
	if password == "INTERNAL" {
		tsconfig := sdk.TSConfig{
			Email:          opt.Email,
			MasterKeys:     strings.Split(opt.MasterKeys, "|"),
			APIKey:         apiKey,
			PublicKey:      opt.PublicKey,
			PrivateKey:     opt.PrivateKey,
			AuthVersion:    opt.AuthVersion,
			BaseFolderUUID: opt.BaseFolderUUID,
		}
		filen, err = sdk.NewFromTSConfig(tsconfig)
		if err != nil {
			return nil, err
		}
	} else {
		filen, err = sdk.NewWithAPIKey(ctx, opt.Email, password, apiKey)
		if err != nil {
			return nil, err
		}
	}

	maybeRootDir, err := filen.FindDirectory(ctx, root)
	if errors.Is(err, fs.ErrorIsFile) { // FsIsFile special case
		var err2 error
		root = pathModule.Dir(root)
		maybeRootDir, err2 = filen.FindDirectory(ctx, root)
		if err2 != nil {
			return nil, err2
		}
	} else if err != nil {
		return nil, err
	}

	fileSystem := &Fs{
		name:  name,
		root:  Directory{},
		filen: filen,
		Enc:   opt.Encoder,
	}

	fileSystem.features = (&fs.Features{
		ReadMimeType: true,
		//WriteMimeType:           true, // requires parsing metadata options, todo later
		CanHaveEmptyDirectories: true,
		// ReadMetadata: true,
		// WriteMetadata: true,
		// ReadDirMetadata ?
		// WriteDirMetadata ?
		//TODO more optional features?
	}).Fill(ctx, fileSystem)

	fileSystem.root = Directory{
		fs:        fileSystem,
		directory: maybeRootDir, // could be null at this point
		path:      root,
	}

	// must return the error from FindDirectory (see FsIsFile)
	return fileSystem, err
}

// Options defines the configuration for this backend
type Options struct {
	Email          string               `config:"Email"`
	Password       string               `config:"Password"`
	APIKey         string               `config:"API Key"`
	Encoder        encoder.MultiEncoder `config:"encoding"`
	MasterKeys     string
	PrivateKey     string
	PublicKey      string
	AuthVersion    int
	BaseFolderUUID string
}

// Fs represents a virtual filesystem mounted on a specific root folder
type Fs struct {
	name     string
	root     Directory
	filen    *sdk.Filen
	Enc      encoder.MultiEncoder
	features *fs.Features
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root.path
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("Filen %s at /%s", f.filen.Email, f.root.String())
}

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return time.Millisecond
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.SHA512)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dir = f.Enc.FromStandardPath(dir)
	// find directory uuid
	directory, err := f.filen.FindDirectory(ctx, f.resolvePath(dir))
	if err != nil {
		return nil, err
	}

	if directory == nil {
		return nil, fs.ErrorDirNotFound
	}

	// read directory content
	files, directories, err := f.filen.ReadDirectory(ctx, directory)
	if err != nil {
		return nil, err
	}
	entries = make(fs.DirEntries, 0, len(files)+len(directories))

	for _, directory := range directories {
		entries = append(entries, &Directory{
			fs:        f,
			path:      pathModule.Join(dir, directory.Name),
			directory: directory,
		})
	}
	for _, file := range files {
		file := &File{
			fs:   f,
			path: pathModule.Join(dir, file.Name),
			file: file,
		}
		entries = append(entries, file)
	}
	return entries, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
//
// If remote points to a directory then it should return
// ErrorIsDir if possible without doing any extra work,
// otherwise ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	remote = f.Enc.FromStandardPath(remote)
	file, err := f.filen.FindFile(ctx, f.resolvePath(remote))
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, fs.ErrorObjectNotFound
	}
	return &File{
		fs:   f,
		path: remote,
		file: file,
	}, nil
}

// Put in to the remote path with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Put should either
// return an error or upload it properly (rather than e.g. calling panic).
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	path := f.Enc.FromStandardPath(src.Remote())
	resolvedPath := f.resolvePath(path)
	modTime := src.ModTime(ctx)
	parent, err := f.filen.FindDirectoryOrCreate(ctx, pathModule.Dir(resolvedPath))
	if err != nil {
		return nil, err
	}
	incompleteFile, err := types.NewIncompleteFile(f.filen.AuthVersion, pathModule.Base(resolvedPath), "", modTime, modTime, parent)
	if err != nil {
		return nil, err
	}
	uploadedFile, err := f.filen.UploadFile(ctx, incompleteFile, in)
	if err != nil {
		return nil, err
	}
	return &File{
		fs:   f,
		path: path,
		file: uploadedFile,
	}, nil
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	dirObj, err := f.filen.FindDirectoryOrCreate(ctx, f.resolvePath(f.Enc.FromStandardPath(dir)))
	if err != nil {
		return err
	}
	if dir == f.root.path {
		f.root.directory = dirObj
	}
	return nil
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	// find directory
	resolvedPath := f.resolvePath(f.Enc.FromStandardPath(dir))
	//if resolvedPath == f.root.path {
	//	return fs.ErrorDirNotFound
	//}
	directory, err := f.filen.FindDirectory(ctx, resolvedPath)
	if err != nil {
		return err
	}
	if directory == nil {
		return errors.New("directory not found")
	}

	files, dirs, err := f.filen.ReadDirectory(ctx, directory)
	if err != nil {
		return err
	}
	if len(files) > 0 || len(dirs) > 0 {
		return errors.New("directory is not empty")
	}

	// trash directory
	err = f.filen.TrashDirectory(ctx, directory)
	if err != nil {
		return err
	}
	return nil
}

// Directory is Filen's directory type
type Directory struct {
	fs        *Fs
	path      string
	directory types.DirectoryInterface
}

// Fs returns read only access to the Fs that this object is part of
func (dir *Directory) Fs() fs.Info {
	return dir.fs
}

// String returns a description of the Object
func (dir *Directory) String() string {
	if dir == nil {
		return "<nil>"
	}
	return dir.Remote()
}

// Remote returns the remote path
func (dir *Directory) Remote() string {
	return dir.fs.Enc.ToStandardPath(dir.path)
}

// ModTime returns the modification date of the file
// It should return a best guess if one isn't available
func (dir *Directory) ModTime(ctx context.Context) time.Time {
	directory, ok := dir.directory.(*types.Directory)
	if !ok {
		return time.Time{} // todo add account creation time?
	}

	if directory.Created.IsZero() {
		obj, err := dir.fs.filen.FindDirectory(ctx, dir.path)
		newDir, ok := obj.(*types.Directory)
		if err != nil || !ok {
			return time.Now()
		}
		directory = newDir
		dir.directory = newDir
	}
	return directory.Created
}

// Size returns the size of the file
//
// filen doesn't have an efficient way to find the size of a directory
func (dir *Directory) Size() int64 {
	return -1
}

// Items returns the count of items in this directory or this
// directory and subdirectories if known, -1 for unknown
func (dir *Directory) Items() int64 {
	return -1
}

// ID returns the internal ID of this directory if known, or
// "" otherwise
func (dir *Directory) ID() string {
	return dir.directory.GetUUID()
}

// File is Filen's normal file
type File struct {
	fs   *Fs
	path string
	file *types.File
}

// Fs returns read only access to the Fs that this object is part of
func (file *File) Fs() fs.Info {
	return file.fs
}

// String returns a description of the Object
func (file *File) String() string {
	if file == nil {
		return "<nil>"
	}
	return file.Remote()
}

// Remote returns the remote path
func (file *File) Remote() string {
	return file.fs.Enc.ToStandardPath(file.path)
}

// ModTime returns the modification date of the file
// It should return a best guess if one isn't available
func (file *File) ModTime(ctx context.Context) time.Time {
	if file.file.LastModified.IsZero() {
		newFile, err := file.fs.filen.FindFile(ctx, file.path)
		if err == nil && newFile != nil {
			file.file = newFile
		}
	}
	return file.file.LastModified
}

// Size returns the size of the file
func (file *File) Size() int64 {
	return int64(file.file.Size)
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (file *File) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if ty != hash.SHA512 {
		return "", hash.ErrUnsupported
	}
	if file.file.Hash == "" {
		foundFile, err := file.fs.filen.FindFile(ctx, file.path)
		if err != nil {
			return "", err
		}
		if foundFile == nil {
			return "", fs.ErrorObjectNotFound
		}
		file.file = foundFile
	}
	return file.file.Hash, nil
}

// Storable says whether this object can be stored
func (file *File) Storable() bool {
	return true
}

// SetModTime sets the metadata on the object to set the modification date
func (file *File) SetModTime(ctx context.Context, t time.Time) error {
	file.file.LastModified = t
	return file.fs.filen.UpdateMeta(ctx, file.file)
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (file *File) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, file.Size())
	// Create variables to hold our options
	var offset int64
	var limit int64 = -1 // -1 means no limit

	// Parse the options
	for _, option := range options {
		switch opt := option.(type) {
		case *fs.RangeOption:
			offset = opt.Start
			limit = opt.End + 1 // +1 because End is inclusive
		}
	}

	// Get the base reader
	readCloser := file.fs.filen.GetDownloadReaderWithOffset(ctx, file.file, int(offset), int(limit))
	return readCloser, nil
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (file *File) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	newModTime := src.ModTime(ctx)
	newIncomplete, err := file.file.NewFromBase(file.fs.filen.AuthVersion)
	if err != nil {
		return err
	}
	newIncomplete.LastModified = newModTime
	newIncomplete.Created = newModTime
	uploadedFile, err := file.fs.filen.UploadFile(ctx, newIncomplete, in)
	if err != nil {
		return err
	}
	file.file = uploadedFile
	return nil
}

// Remove this object
func (file *File) Remove(ctx context.Context) error {
	if file.file == nil {
		return nil
	}
	err := file.fs.filen.TrashFile(ctx, *file.file)
	if err != nil {
		return err
	}
	return nil
}

// MimeType returns the content type of the Object if
// known, or "" if not
func (file *File) MimeType(_ context.Context) string {
	return file.file.MimeType
}

// ID returns the ID of the Object if known, or "" if not
func (file *File) ID() string {
	return file.file.GetUUID()
}

// ParentID returns the ID of the parent directory if known or nil if not
func (file *File) ParentID() string {
	return file.file.GetParent()
}

// Purge all files in the directory specified
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist
func (f *Fs) Purge(ctx context.Context, dir string) error {
	path := f.resolvePath(f.Enc.FromStandardPath(dir))
	foundDir, err := f.filen.FindDirectory(ctx, path)
	if err != nil {
		return err
	} else if foundDir == nil {
		return fs.ErrorDirNotFound
	}
	return f.filen.TrashDirectory(ctx, foundDir)
}

// Move src to this remote using server-side move operations.
//
// # This is stored with the remote path given
//
// # It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	obj, ok := src.(*File)
	if !ok {
		return nil, fmt.Errorf("can't move %T: %w", src, fs.ErrorCantMove)
	}
	newRemote := f.Enc.FromStandardPath(remote)
	oldPath, newPath := obj.fs.resolvePath(f.Enc.FromStandardPath(src.Remote())), f.resolvePath(newRemote)
	oldParentPath, newParentPath := getPathDir(oldPath), getPathDir(newPath)
	oldName, newName := pathModule.Base(oldPath), pathModule.Base(newPath)
	var err error
	if oldPath == newPath {
		return nil, fs.ErrorCantMove
	} else if oldParentPath == newParentPath {
		err = f.rename(ctx, obj.file, newPath, newName)
	} else if newName == oldName {
		err = f.move(ctx, obj.file, newPath, newParentPath)
	} else {
		err = f.moveWithRename(ctx, obj.file, oldPath, oldName, newPath, newParentPath, newName)
	}
	if err != nil {
		return nil, err
	}
	return moveFileObjIntoNewPath(obj, newRemote), nil
}

// moveWithRename moves item to newPath
// using a more complex set of operations designed to handle the fact that
// Filen doesn't support a single moveRename operation
// which requires some annoying hackery to get around reliably
func (f *Fs) moveWithRename(ctx context.Context, item types.NonRootFileSystemObject, oldPath, oldName, newPath, newParentPath, newName string) error {
	g, gCtx := errgroup.WithContext(ctx)
	var (
		newParentDir  types.DirectoryInterface
		renamedToUUID bool
	)

	// rename to random UUID first
	g.Go(func() error {
		var err error
		err = f.filen.Rename(gCtx, item, uuid.NewString())
		if err != nil {
			return fmt.Errorf("failed to rename file: %w : %w", err, fs.ErrorCantMove)
		}
		renamedToUUID = true
		return nil
	})
	defer func() {
		// safety to try and not leave the item in a bad state
		if renamedToUUID {
			err := f.filen.Rename(ctx, item, oldName)
			if err != nil {
				fmt.Printf("ERROR: FAILED TO REVERT UUID RENAME for file %s: %s", oldPath, err)
			}
		}
	}()

	// find parent dir
	g.Go(func() error {
		var err error
		newParentDir, err = f.filen.FindDirectoryOrCreate(gCtx, newParentPath)
		return err
	})

	if err := g.Wait(); err != nil {
		return err
	}

	// move
	oldParentUUID := item.GetParent()
	err := f.filen.MoveItem(ctx, item, newParentDir.GetUUID(), true)
	if err != nil {
		return fmt.Errorf("failed to move file: %w : %w", err, fs.ErrorCantMove)
	}
	defer func() {
		// safety to try and not leave the item in a bad state
		if renamedToUUID {
			err := f.filen.MoveItem(ctx, item, oldParentUUID, true)
			if err != nil {
				fmt.Printf("ERROR: FAILED TO REVERT MOVE for file %s: %s", oldPath, err)
			}
		}
	}()

	// rename to final name
	err = f.filen.Rename(ctx, item, newName)
	if err != nil {
		return fmt.Errorf("failed to rename file: %w : %w", err, fs.ErrorCantMove)
	}
	renamedToUUID = false

	return nil
}

// move moves item to newPath
// by finding the parent and calling moveWithParentUUID
func (f *Fs) move(ctx context.Context, item types.NonRootFileSystemObject, newPath, newParentPath string) error {
	newParentDir, err := f.filen.FindDirectoryOrCreate(ctx, newParentPath)
	if err != nil {
		return fmt.Errorf("failed to find or create directory: %w : %w", err, fs.ErrorCantMove)
	}
	return f.moveWithParentUUID(ctx, item, newParentDir.GetUUID())
}

// moveWithParentUUID moves item to newParentUUID
// using a simple filen.MoveItem operation
func (f *Fs) moveWithParentUUID(ctx context.Context, item types.NonRootFileSystemObject, newParentUUID string) error {
	err := f.filen.MoveItem(ctx, item, newParentUUID, true)
	if err != nil {
		return fmt.Errorf("failed to move file: %w : %w", err, fs.ErrorCantMove)
	}

	return nil
}

// rename moves item to newPath
// using a simple Filen rename operation
func (f *Fs) rename(ctx context.Context, item types.NonRootFileSystemObject, newPath string, newName string) error {
	err := f.filen.Rename(ctx, item, newName)
	if err != nil {
		return fmt.Errorf("failed to rename item: %w : %w", err, fs.ErrorCantMove)
	}
	return nil
}

// moveFileObjIntoNewPath 'moves' an existing object into a new path
// invalidating the previous object
// and making a copy with the passed path
//
// this is to work around the fact that rclone expects to have to delete a file after moving
func moveFileObjIntoNewPath(obj *File, newPath string) *File {
	newFile := &File{
		fs:   obj.fs,
		path: newPath,
		file: obj.file,
	}
	obj.file = nil
	return newFile
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {

	srcF, ok := src.(*Fs)
	if !ok || srcF == nil {
		return fs.ErrorCantDirMove
	}
	g, gCtx := errgroup.WithContext(ctx)
	var (
		srcDirInt types.DirectoryInterface
		dstDir    types.DirectoryInterface
		srcPath   = srcF.resolvePath(srcF.Enc.FromStandardPath(srcRemote))
		dstPath   = f.resolvePath(f.Enc.FromStandardPath(dstRemote))
	)
	if srcPath == dstPath {
		return fs.ErrorDirExists
	}

	g.Go(func() error {
		var err error
		srcDirInt, err = srcF.filen.FindDirectory(gCtx, srcPath)
		return err
	})
	g.Go(func() error {
		var err error
		dstDir, err = f.filen.FindDirectory(gCtx, dstPath)
		return err
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if srcDirInt == nil {
		return fs.ErrorDirNotFound
	}

	if dstDir != nil {
		return f.dirMoveContents(ctx, srcDirInt, dstDir, srcPath, dstPath)
	}

	srcDir, ok := srcDirInt.(*types.Directory)
	if !ok {
		return fs.ErrorCantDirMove
	}

	return f.dirMoveEntireDir(ctx, srcDir, srcPath, dstPath)
}

// dirMoveContents moves the contents of srcDir to dstDir
// used for the case where the target directory exists
// recurses if needed
func (f *Fs) dirMoveContents(ctx context.Context, srcDir, dstDir types.DirectoryInterface, srcPath, dstPath string) error {
	g, gCtx := errgroup.WithContext(ctx)
	var (
		srcDirs  []*types.Directory
		srcFiles []*types.File
		dstDirs  []*types.Directory
		dstFiles []*types.File
	)

	// read source and target
	g.Go(func() error {
		var err error
		srcFiles, srcDirs, err = f.filen.ReadDirectory(gCtx, srcDir)
		return err
	})
	g.Go(func() error {
		var err error
		dstFiles, dstDirs, err = f.filen.ReadDirectory(gCtx, dstDir)
		return err
	})

	if err := g.Wait(); err != nil {
		return err
	}

	dstDirNamesSet := make(map[string]*types.Directory, len(dstDirs)+len(dstFiles))
	for _, dir := range dstDirs {
		dstDirNamesSet[dir.GetName()] = dir
	}

	g, gCtx = errgroup.WithContext(ctx)
	g.SetLimit(sdk.MaxSmallCallers)

	for _, dir := range srcDirs {
		currSrcPath := pathModule.Join(srcPath, dir.GetName())
		currDstPath := pathModule.Join(dstPath, dir.GetName())
		if dupDir, ok := dstDirNamesSet[dir.GetName()]; ok {
			// if duplicate, recurse
			g.Go(func() error {
				return f.dirMoveContents(gCtx, dir, dupDir, currSrcPath, currDstPath)
			})
		} else {
			// else move
			g.Go(func() error {
				return f.moveWithParentUUID(gCtx, dir, dstDir.GetUUID())
			})
		}
	}

	for _, file := range srcFiles {
		// move all files with overwrite
		g.Go(func() error {
			return f.moveWithParentUUID(gCtx, file, dstDir.GetUUID())
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

// dirMoveEntireDir moves srcDir to newPath
// used for the case where the target directory doesn't exist
func (f *Fs) dirMoveEntireDir(ctx context.Context, srcDir *types.Directory, oldPath string, newPath string) error {
	oldParentPath, newParentPath := getPathDir(oldPath), getPathDir(newPath)
	oldName, newName := pathModule.Base(oldPath), pathModule.Base(newPath)
	var err error
	if oldPath == newPath {
		return fs.ErrorDirExists
	} else if oldParentPath == newParentPath {
		err = f.rename(ctx, srcDir, newPath, newName)
	} else if newName == oldName {
		err = f.move(ctx, srcDir, newPath, newParentPath)
	} else {
		err = f.moveWithRename(ctx, srcDir, oldPath, oldName, newPath, newParentPath, newName)
	}
	if err != nil {
		return err
	}
	return err
}

// helpers

// resolvePath returns the absolute path specified by the input path, which is seen relative to the remote's root.
func (f *Fs) resolvePath(path string) string {
	return pathModule.Join(f.root.path, path)
}

func getPathDir(path string) string {
	dir := pathModule.Dir(path)
	if dir == "." {
		return ""
	}
	return dir
}

// Check the interfaces are satisfied
var (
	_ fs.Fs        = (*Fs)(nil)
	_ fs.Directory = (*Directory)(nil)
	//_ fs.SetModTimer = (*Directory)(nil) todo
	_ fs.Object     = (*File)(nil)
	_ fs.MimeTyper  = (*File)(nil)
	_ fs.IDer       = (*File)(nil)
	_ fs.ParentIDer = (*File)(nil)
)
