#StreamWatcher Model#
'''
Created on 16/03/2011

@author: team A

04/12/2013: Fixed by DivineSeeders: Assaf Krintza and Nir Malbin

'''
import time
from sys import stdout 
import sys
import os.path
import csv
import os
import traceback
import math

class StreamWatcher:
    def __init__(self, _sched ,
                       _downloader, 
                       _storagewrapper ,
                       _config,_downmeasure):
        
        self.sched = _sched                              #rawserver tasks queue Instance
        self.downloader = _downloader                    #downloader Instance
        self.storagewrapper = _storagewrapper            #StorageWrapper Instance
        self.config = _config                            #Config Dictionary (Olds Argv)
        self.downmeasure = _downmeasure 
        self.init()                                      #Initialize self Instance

      
    def init (self):
        self.rate  = int(self.config['rate']) 
        self.delay = int(self.config['delay'])
        self.prefetchT  = int(self.config['prefetchT']) 
        self.total_dfs = 0                               #Total No. of bytes taken from server
        self.p2p = 0                   #Total No. of bytes downloaded so far             
        self.piece_size = self.storagewrapper.piece_size #"this" torrent piece size in Bytes
        self.total = self.storagewrapper.total_length    #"this" torrent total size in Bytes
        self.numOfPieces = len(self.storagewrapper.have) #"this" torrent number of pieces
        self.numOfFullPiecesFromServer  =  0
        self.numOfDirtyPiecesFromServer =  0
        self.numOFFaildHashChecksFromServer = 0
        self.startTime = time.time()                    #StreamWatcher Start Time
        stdout.flush()
        self.gap=int(self.config['gap'])
        self.init_csv(self.config['out_dir']+'statistics-order-'+self.config['order']+'-gap-'+str(self.gap)+'.csv')    
        self.prefetch = int(((float(self.prefetchT) / 100)*self.toKbytes(self.total))/self.rate)
        self.stats = [] #DivineSeeders: The statistics data in memory
    
    def init_csv (self, csv):
        self.csvFile = csv
        try:
            if (os.path.exists(self.csvFile)):
                os.remove(self.csvFile)  
        except ValueError:
            return

    def verify_vod_rate(self):
        try:
            t = int(time.time() - self.startTime)
            Orig  =  int(((t - self.delay) * self.rate) / self.toKbytes(self.piece_size))
            Dest  =  int(((t - self.delay + self.prefetch ) * self.rate) / self.toKbytes(self.piece_size))
            #Orig & Dest are pieces indexes
            #(Calculation is done in KB to match self.rate which is given in KB)                                 
            if Dest>self.numOfPieces:
                Dest = self.numOfPieces
            if Orig > Dest:
                Orig = Dest
       
            if (not self.storagewrapper.am_I_complete()):
                #Loop over the gap [Orig,Dest] to check this peer 'have' list:
                for i in range(Orig,Dest):
                    #Case 1 : piece wasn't downloaded at all till now and no pending request for it also:
                    if (self.storagewrapper.is_unstarted(i)):
                            self.total_dfs += self.piece_size
                            self.numOfFullPiecesFromServer +=1
                    #Case 2 : piece is in the middle of a download from some peer\seed: 
                    else:    
                        if ((not self.storagewrapper.do_I_have(i)) and self.storagewrapper.dirty.has_key(i)):
                            holes = self.get_dirty_holes (self.storagewrapper.dirty[i])
                            if (holes):
                                self.cancel_piece_download(i)
                                j = iter(holes)
                                counter = len(holes)
                                while(counter>1):
                                    chunk  = j.next()
                                    self.total_dfs += chunk[1]
                                    counter-=1
                                chunk  = j.next()
                                self.total_dfs += chunk[1]
                                self.numOfDirtyPiecesFromServer +=1   
            
            dfs = (self.total_dfs*100)/self.total
            
            #DivineSeeders: store the statistics in memory
            self.stats.append((t, dfs, self.p2p))
#             self.stats2csv(dfs, self.p2p)
#             print ''
#             print 'DEBUG: t:',t,' order:',self.config['order'],"checked: [",Orig,",",Dest,"] write: (",dfs,",",self.p2p,")"
#             print ''
#             stdout.flush()
            
            if(Dest == self.numOfPieces):
                # DivineSeeders: terminate this client after the gap
                self.sched(self.test_completed, (int(self.config['group_size']) - int(self.config['order'])) * self.gap * 16)
            else:
                self.sched(self.verify_vod_rate, self.prefetch)
        except:
            traceback.print_exc()

    # DivineSeeders: When test is over spill the stats to CSV and
    # exit with test-success code (3)
    def test_completed(self):
        try:
            if self.stats2csv():
                stdout.flush()
                os._exit(3)
                return
        except:
            traceback.print_exc()
        
        stdout.flush()
        os._exit(0)

    def get_dirty_holes(self,dirty):
        if (not dirty):
            return None
        try:
            holes = []
            chunk_size = dirty[0][1]
            j=0
            while (j!=self.piece_size):
                h = (j,chunk_size)
                try:
                    dirty.index(h)
                except ValueError:
                    holes.append(h)
                j+=chunk_size
            return holes
        except ValueError:
            return None
       
    def cancel_piece_download(self,index):
        pieceToCnacel = []
        pieceToCnacel.append(index)
        #Cancel all pending downloading request for this piece:
        self.downloader.cancel_piece_download(pieceToCnacel)
        
        
    def display(self) :
        if self.config['verbose']: 
            t = int(time.time() - self.startTime)
            cur_piece = int(((t - self.delay) * self.rate) / self.toKbytes(self.piece_size))
            
            print '--------------------------------StreamWatcher-------------------------------------\r'
            print 'Csv stats:        ',  self.csvFile,'\r'
            print 'DFS is:           ',  self.total_dfs                 ,'bytes\r'
            print 'DFS/Total is:     ',  (self.total_dfs*100)/self.total ,'%\r'
            print 'FullPieces:       ',  self.numOfFullPiecesFromServer ,'/',self.numOfPieces ,'\r'
            print 'DirtyPieces:      ',  self.numOfDirtyPiecesFromServer ,'/',self.numOfPieces,'\r'
            print 'FaildHashChecks:  ',  self.numOFFaildHashChecksFromServer,'\r'
            print 'Prefetching       ',  self.config['prefetchT'],'%\r'
            if cur_piece < 0:
                cur_piece = 0
            print 'Playing point:    ',  cur_piece,'/',self.numOfPieces,'(',int(((cur_piece*100)/self.numOfPieces)),'%)\r'
            stdout.flush()


    # DivineSeeders: writing the stats to CSV file
    def stats2csv(self):
        group_size = int(self.config['group_size'])
        order = int(self.config['order'])
        end_time = int(self.toKbytes(self.total) / self.rate)
        
        print "CSV: order ",order,"has",len(self.stats)
        
        if (len(self.stats) < (end_time / self.prefetch) * 0.5):
            return 0
        
        with open(self.csvFile, 'wb') as file:
            writer = csv.writer(file)
            writer.writerow(['alg','dfs','p2p'])
            
            #pre-gap padding
            for i in range(0, (order - 1) * self.gap):
                #print "DEBUG: order ",order,"pre-gap",i,"dfs",0,"p2p",0
                writer.writerow([self.config['alg'],0,0])
            
            #my data
            t = self.stats[0][0]
            statsIndex = 0
            
            while (t <= end_time) :
                while t >= self.stats[statsIndex][0]:
                    lastStats = self.stats[statsIndex]
                    statsIndex = statsIndex + 1
                    
                factor =  float(t - lastStats[0]) / (self.stats[statsIndex][0] - lastStats[0])
                dfs = int(math.ceil(lastStats[1] + factor * (self.stats[statsIndex][1] - lastStats[1])))
                p2p = int(math.ceil(lastStats[2] + factor * (self.stats[statsIndex][2] - lastStats[2])))
                
                #print "DEBUG: order ",order,"t",t,"index",statsIndex,"factor",factor,"dfs",dfs,"p2p",p2p
                
                writer.writerow([self.config['alg'],int(dfs),int(p2p)])
                
                t = t + self.prefetch
            
            lastStats = self.stats[len(self.stats) - 1]
            
            #print "DEBUG: order ",order,"t",t,"index",statsIndex,"dfs",lastStats[1],"p2p",lastStats[2]
            writer.writerow([self.config['alg'],lastStats[1],lastStats[2]])
            
            #post-gap padding
            for i in range(0, (group_size - order) * self.gap):
                #print "DEBUG: order ",order,"post-gap",i,"dfs",lastStats[1],"p2p",lastStats[2]
                writer.writerow([self.config['alg'],lastStats[1],lastStats[2]])
            
            file.flush()
        
        return 1

#    # DivineSeeders: old CSV writing...
#     def stats2csv(self,dfs,p2p):  
#         try:
#             if (not os.path.exists(self.csvFile)):
#                 csvFile = open(self.csvFile, 'wb')
#                 FcsvWriter = csv.writer(csvFile)
#                 FcsvWriter.writerow(['alg','dfs','p2p'])
#                 no_data=0
#                 order = int(self.config['order'])-1
#                 while(order>0):
#                         gap=self.gap
#                         while(gap>0):
#                                 FcsvWriter.writerow([self.config['alg'],no_data,no_data])
#                                 gap = gap-1
#                         order = order-1
#                 FcsvWriter.writerow([self.config['alg'],dfs,p2p])
#                 csvFile.flush()
#                 csvFile.close()
#             else:
#                 csvFile = open(self.csvFile, 'a')
#                 FcsvWriter = csv.writer(csvFile)
#                 FcsvWriter.writerow([self.config['alg'],dfs,p2p])
#                 csvFile.flush()
#                 csvFile.close()
#         except (IOError, OSError), e:
#             print "IO Error:" + str(e)

    def toBytes (self,x):
        return int(x*1024)
    
    def toKbytes (self,x):
        return int(x/1024)
    
    
