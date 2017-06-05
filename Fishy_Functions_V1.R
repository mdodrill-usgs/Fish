################################################################################
#
#                  Functions for dealing with fishy data
#
#  Added: 6/2/2017
#  * 'update. db', function which updates the data, by writing the latest tables
#  from access (found on the M:/ drive) using the 'exporter' to a new sql.lite db.
#  
#  Added: 6/3/2017
#  * get.fish.dat, function that grabs data from the db
#    --> need to set this up for other gears with an argument like 
#
#  Created: 6/2/2017
# 
################################################################################
# get the directory from where the script lives (not using Rstudio to 'source',
# or when you use source in a script)
str.dir = getSrcDirectory(function(x) {x})

data.dir = paste(str.dir, "/Data", sep = "")

#-----------------------------------------------------------------------------#
#------------------- update.db -----------------------------------------------#
# 6/2/2017
# this function will update the fish data by finding the access database on the
# M: drive and using Glen's export tool, write a new sqlite database


update.db = function(){
  require(RSQLite)
  require(dplyr)
  
  # data.dir = paste(str.dir, "/Data", sep = "")
  # common_wd = paste(str.dir, "/Data", sep = "")
  
  db.dir = "M:/DATABASE/BIOLOGICAL/FISH/DATABASE_BACKUPS"
  
  if(dir.exists(path = db.dir) == FALSE){
    return(message("MD: Can't find the database!, Are you connected to the M drive?"))
  }
  
  
  all.files = list.files(path = db.dir, pattern = ".mdb")
  
  # find the most recent db
  the.one = sort(all.files)[length(all.files)]
  the.one.date = substr(the.one, 30, nchar(the.one) - 4)
  
  db.path = paste(db.dir, the.one, sep = "/")
  
  
  # today = format(Sys.Date(), "%m_%d_%Y")
  
  # the "AccessExporter" takes 3 arguments. 1) Path to the access database, 2) the
  # name of the table that you want, 3) path & name of the file to write (table to
  # export), this can be either a .csv or .txt
  
  # the first argument to system is the location & name of the "AccessExporter",
  # here "AccessExporter_v1.exe"
  
  # sample table
  file.name = paste(data.dir, "/samp_table_", the.one.date, ".txt", sep = "")
  
  exporter.path = paste(str.dir, "/Exporter/AccessExporter_v1.exe", sep = "")
  
  system(paste(exporter.path,
               db.path,
               "FISH_T_SAMPLE",
               file.name))
  #---------------------------------
  # specimen table
  file.name.2 = paste(data.dir, "/spec_table_", the.one.date, ".txt", sep = "")
  
  system(paste(exporter.path,
               db.path,
               "FISH_T_SPECIMEN",
               file.name.2))
  
  #-----------------------------------------------------------------------------#
  # create the new db
  
  samp = read.table(file = file.name, header = T, sep = ",")
  
  spec = read.table(file = file.name.2, header = T, sep = ",")
  
  db.name = paste(data.dir, "/my_db_", the.one.date, ".sqlite3", sep = "")
  
  # create a blank database 
  my_db <- src_sqlite(db.name, create = T)
  
  # addes these tables to the "my_db"
  copy_to(my_db, samp, temporary = FALSE)
  copy_to(my_db, spec, temporary = FALSE)
  
  # remove the .txt files 
  file.remove(file.name)
  file.remove(file.name.2)
  
  
  # dat$DriftDate = strptime(dat$DriftDate, format = "%m/%d/%Y")
  # 
  # # fix the times
  # tmp = strptime(dat$TimeBegin, format = "%m/%d/%Y %H:%M:%S")
  # 
  # dat$TimeBegin = strftime(tmp, format = "%R")
  
  # remove redflag col for now (if not, this will trip off the template check later)
  # dat2 = dat[,seq(1,c(ncol(dat) - 2))]
  
  # write.csv(dat2, file = file.name, row.names = F)
  
  #-----------------------------------------------------------------------------#
  message("MD: Update Successful")
  return()
}



#-----------------------------------------------------------------------------#
get.fish.dat = function(){
  library(RSQLite)
  library(dplyr)
  
  # path to sqlite db (there should only be one db in the folder)
  db.name = list.files(path = data.dir, pattern = ".sqlite3")
  my_path = paste(data.dir, db.name, sep = "/")
  
  # connect to db
  my_db = src_sqlite(path = my_path, create = FALSE)
  
  # get the tables 
  db_fish_samp = tbl(my_db, "samp")
  db_fish_spec = tbl(my_db, "spec")
  #-----------------------------------------------------------------------------#
  # set up fish samples
  
  samp.cols = c("SAMPLE_ID",
                "SAMPLE_TYPE",
                "TRIP_ID",
                "START_DATETIME",
                "END_DATETIME",
                "SAMPLE_TYPE",
                "RIVER_CODE",
                "START_RM",
                #  "END_RM",  # not really used with seines 
                #  "START_RKM",
                #  "END_RKM",
                "GEAR_CODE",
                "EFFORT",
                "TOTAL_CATCH",
                "SEINE_HAUL_LENGTH",
                "SEINE_TOTAL_AREA",
                "SEINE_HAB_WIDTH",
                "SEINE_HAB_LENGTH",
                "SEINE_DEPTH_1",
                "SEINE_DEPTH_2",
                "SEINE_MAX_DEPTH",
                "DEPLETION_NUM",
                "HABITAT_CODE",
                "HYDRAULIC_CODE",
                "SUBSTRATE_CODE",
                "COVER_CODE",
                "TURBIDITY",
                "WATER_TEMP",
                "AIR_TEMP",
                "SAMPLE_NOTES",
                # "STUDY_OBJECTIVE_CODE", # all NA
                # "TURBIDITY_NTU", # all NA
                "STATION_ID")
  
  # 
  all.sn = c('BL','BS','BX','SA','SB','SC','SEN','SG','SL','SS','SX','SN')
  
  sa1 = select(db_fish_samp, one_of(samp.cols)) %>% 
    filter(GEAR_CODE %in% all.sn, !is.na(START_DATETIME),
           !is.na(START_RM))     
  
  sa2 = collect(sa1)
  str(sa2)
  
  #-----------------------------------------------------------------------------#
  # set up fish specimens
  
  spec.cols = c("SAMPLE_ID",
                "SPECIES_CODE",
                "TOTAL_LENGTH",
                "FORK_LENGTH",
                #  "DEPLETION_HAUL_NUMBER",  # all NA
                "DISPOSITION_CODE")
  
  sp1 = select(db_fish_spec, one_of(spec.cols))# %>% 
  
  sp2 = collect(sp1)
  sp2
  
  #-----------------------------------------------------------------------------#
  # Join the tables
  d1 = inner_join(sa2, sp2, by = "SAMPLE_ID")
  d1
  
  # columns to lower case
  colnames(d1) = tolower(colnames(d1))
  
  return(data = d1)
}


### END
#-----------------------------------------------------------------------------#
### END ###### END ###### END ###### END ###### END ###### END ###### END ###### END ###