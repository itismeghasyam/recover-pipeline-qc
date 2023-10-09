########
# Required Libraries and functions
########
library(arrow)
library(synapser)
library(tidyverse)
library(synapserutils)
source('~/recover-s3-synindex/awscli_utils.R')
source('~/recover-s3-synindex/params.R')

getFileType <- function(file_name){
  file_name <- stringr::str_replace_all(file_name,'./temp_location/','')
  file_name_split <- stringr::str_split(file_name,'_')[[1]]
  
  if(length(file_name_split)>1){
    file_name_split <- file_name_split[-length(file_name_split)]
  }
  return(paste0(file_name_split,collapse = ''))
}

unzipFile <- function(file_path, target_path, file_list_return= TRUE){
  # uses system unzip instead of inbuilt R function
  # the target path and file path are relative to the working directory. So let's make them absolute paths
  # ie './testt' to '/home/ubuntu/recover-s3-synindex/testt'
  
  # curr_wd <- getwd()
  file_path <-  substr(file_path,2,nchar(file_path)) # removing the first char of '.'
  target_path <- substr(target_path,2,nchar(target_path))
  # print(file_path)
  
  file_path <- str_replace(file_path,'\\\\','\\\\\\\\')
  target_path <- str_replace(target_path,'\\\\','\\\\\\\\')
  
  # file_path <- fs::path_abs(file_path)
  # target_path <- fs::path_abs(target_path)
  
  file_path <- c(getwd(),file_path, sep = '') %>% paste0(collapse = '')
  target_path <- c(getwd(), target_path,sep = '') %>% paste0(collapse = '')
  
  
  # print(file_path)
  
  command_in <- paste0('unzip -q ',file_path,' -d ',target_path)
  # print(command_in)
  system(command_in)
  if(file_list_return){
    file_list <- list.files(target_path)
    return(file_list)
  }
}

########
# Set up Access and download dataset
########
synapser::synLogin()

########## INGRESS BUCKET
sts_token <- synapser::synGetStsStorageToken(entity = 'syn52293299', # sts enabled destination folder
                                             permission = 'read_write',  
                                             output_format = 'json')

# configure the environment with AWS token (this is the aws_profile named 'env-var')
Sys.setenv('AWS_ACCESS_KEY_ID'=sts_token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=sts_token$secretAccessKey,
           'AWS_SESSION_TOKEN'=sts_token$sessionToken)

# Sync Ingress bucket
s3SyncToLocal(source_bucket = paste0('s3://', INGRESS_BUCKET,'/'),
              local_destination = AWS_DOWNLOAD_LOCATION,
              aws_profile = 'env-var') 

# list objects from INGRESS_BUCKET
s3lsBucketObjects(source_bucket =  paste0('s3://', INGRESS_BUCKET),
                  aws_profile = 'env-var')
ingress_bucket_objects <- data.table::fread('s3files.txt') %>% 
  `colnames<-`(c('date','time','size','location')) %>% 
  dplyr::mutate(ingress_date = lubridate::as_datetime(paste0(date," ",time))) %>% 
  dplyr::select(ingress_date, size, ingress_location = location) %>% 
  as.data.frame() %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(expected_preETL_location = paste0('main/',stringr::str_replace(ingress_location,'\\\\','_'))) %>% 
  dplyr::ungroup()

########## PRE ETL BUCKET
sts_token <- synapser::synGetStsStorageToken(entity = 'syn51714264', # sts enabled destination folder
                                             permission = 'read_write',  
                                             output_format = 'json')

# configure the environment with AWS token (this is the aws_profile named 'env-var')
Sys.setenv('AWS_ACCESS_KEY_ID'=sts_token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=sts_token$secretAccessKey,
           'AWS_SESSION_TOKEN'=sts_token$sessionToken)

# list objects from PRE_ETL_BUCKET
s3lsBucketObjects(source_bucket =  paste0('s3://', PRE_ETL_BUCKET, '/main'),
                  aws_profile = 'env-var')

preETL_bucket_objects <- data.table::fread('s3files.txt') %>% 
  `colnames<-`(c('date','time','size','location')) %>% 
  dplyr::filter(size > 0) %>%  # this removes locations like "main/adults_v1/" that are size 0, the files inside the folder and its subfolder are not affected
  dplyr::mutate(preETL_date = lubridate::as_datetime(paste0(date," ",time))) %>% 
  dplyr::select(preETL_date, size, preETL_location = location) %>% 
  as.data.frame()

ingress_preETL_filemap <- preETL_bucket_objects %>% 
  dplyr::full_join(ingress_bucket_objects %>% 
                     dplyr::rename(preETL_location = expected_preETL_location)) %>% 
  unique() %>% 
  dplyr::filter(size > 400) %>%  # remove owner.txt and test_rtf.pdf 
  dplyr::filter(!is.na(preETL_date)) # don't consider objects that have not been through ETL pipeline 

## To tally ingress and preETL buckets.
write.csv(ingress_preETL_filemap, 'ingress_preETL_fileMap.csv')

############
# Get list of files inside each zip file
############

# To subset the for loop below, i.e look into specific zip files
ROW_START = 1
ROW_STOP = 3

ingress_preETL_filemap_in <- ingress_preETL_filemap %>% 
  dplyr::filter(!ingress_location %in% aa_meta$zipFileLocation)


files.meta <- apply(ingress_preETL_filemap_in[ROW_START:ROW_STOP,],1,function(x){
  temp_file_path <- paste0(AWS_DOWNLOAD_LOCATION, x[['ingress_location']])
  temp_location <- './temp_location'
  unlink(temp_location, recursive = TRUE)
  
  # print(temp_file_path)
  
  tryCatch({
    ## list of files from all the zip files under the local copy of ingress data
    file_list <- unzipFile(file_path = temp_file_path, target_path = temp_location,file_list_return = TRUE) %>% 
      as.data.frame() 
    
    file_list <- list.files(temp_location)
    
    file_set <- lapply(file_list, function(file_unzipped){
      # files_file_location <- file_list[grepl('EnrolledParticipants',file_list)]
      files_file_location <- file_unzipped
      # print(files_file_location)
      datasetType <- paste0('dataset_',tolower(getFileType(files_file_location)))
      files_file_location_full <- paste0(temp_location,'/', files_file_location)
      # print(datasetType)
      
      file_info <- NULL
      
      # We deal with the below datasets (fitbit intraday combined) in a seperate loop,
      # as my instance cannot handle such big datasets.
      
      if(datasetType %in% c(
        'dataset_fitbitintradaycombined'
                             # ,
                            # 'dataset_healthkitv2samplesheartrate',
                            # 'dataset_healthkitv2samplesheartratedeleted'
                            )){
        files_file <- 'NA'
        file_info <- data.frame(nRows =  NA,
                                zipFileLocation = x[['ingress_location']],
                                fileInsideDataset = files_file_location)
        print(paste0(datasetType,'--SKIP'))
      }else if(!file_unzipped == 'Manifest.csv'){
        files_file <- ndjson::stream_in(files_file_location_full)
        print(paste0(datasetType,'--read'))
      }else{
        files_file <- read.csv(files_file_location_full)
        print(paste0(datasetType,'--read2'))
      }
      

      if(is.null(file_info)){
        file_info <- data.frame(nRows =  nrow(files_file),
                                zipFileLocation = x[['ingress_location']],
                                fileInsideDataset = files_file_location)
      }
      rm(files_file)
      return(file_info)
      
    }) %>% data.table::rbindlist(fill= TRUE)
    
    print(paste0('DONE - ', temp_file_path))
    
    return(file_set)
    
  }, error = function(e){
    print(paste0('!!!ERROR!!! - ', temp_file_path))
    return(data.frame(nRows =  'FAIL',
                      zipFileLocation = x[['ingress_location']],
                      fileInsideDataset = 'FAIL'))
  })
}) 

files.meta <- files.meta %>%
  data.table::rbindlist(fill = TRUE)


files.meta.tbl <- files.meta %>% 
  as.data.frame() %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(fileType = getFileType(fileInsideDataset)) %>% 
  dplyr::ungroup()


file_name <- paste0('files_ingress_row_count_', ROW_START, '_' ,ROW_STOP,'_sub.csv')

write.csv(files.meta.tbl, file_name)

# list of file types (FitbitIntradayCombined, EnrolledParticipants, Manifest.csv etc.,) vs zip file
fileType_tally <- table(files.meta.tbl$fileType)
View(fileType_tally)

#################
### Merge all the tally datasets till now
#################

## list all files and subset to those that start like files_ingress_row_count
file.list <- list.files('.')

to_merge.list <- file.list[grepl('files_ingress_row',file.list)]

aa <- lapply(to_merge.list, function(file_in){
  print(file_in)
  return(read.csv(file_in) %>% 
           na.omit())
}) %>% data.table::rbindlist(fill = T) %>% 
  dplyr::select(-X)

aa_meta <- aa %>% 
  unique() %>% 
  dplyr::filter(!fileType == 'FitbitIntradayCombined') %>%  # deal with this dataset type separately
  na.omit() 

write.csv(aa_meta, file = 'ingress_zip_json_row_count.csv')

#################
## Upload to Synapse
#################
parent_synapse_id <- 'syn52617786' # Ingress preETL fileMap
synStore(File('ingress_preETL_fileMap.csv', parentId=parent_synapse_id))

parent_synapse_id <- 'syn52202326' # Ingress preETL fileMap
synStore(File('ingress_zip_json_row_count.csv', parentId=parent_synapse_id))


