#!/usr/bin/python
from google.cloud import storage
from operator import attrgetter
from dateutil.parser import parse
import re, datetime

def format_bytes(size, suffix='B'):
    # 2**10 = 1024
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(size) < 1024.0:
            return "%3.1f%s%s" % (size, unit, suffix)
        size /= 1024.0
    return "%.1f%s%s" % (size, 'Yi', suffix)

def mean_size_selected_files(filesize, file_count):
    mean = format_bytes(filesize/file_count)
    return mean

if __name__ == "__main__":
    # Set by your own env values
    service_account="./sa/amarbank-sre-owner.json" # service account path location
    project_id="amarbank-production-buckets" # gcp project id where bucket located
    bucket_name="prod_ab_crmf"
    regex_filter=r'selfie' # Let it blank to print all
    date_limitator=datetime.datetime(2021, 7, 23) # Let it blank to print all

    # Let this vars by default
    all_filtered_blobs = []
    size_only_blobs = []
    count = 0
    total_size = 0
    
    client = storage.Client.from_service_account_json(service_account, project=project_id)

    all_raw_blobs = list(sorted(client.list_blobs(bucket_name), key=attrgetter('size')))    
    regexp = re.compile(regex_filter)

    for blob in all_raw_blobs:
      blob_date=datetime.datetime(int(parse(str(blob.time_created)).year), int(parse(str(blob.time_created)).month), int(parse(str(blob.time_created)).day))  
      if regexp.search(blob.name) and blob_date > date_limitator:              # Filter blob objects based on regex rules above
          all_filtered_blobs.append(blob)       # Add the filtered blob objects to new list variable
          size_only_blobs.append(blob.size)     # List all blob size objects to get Max and Min values
          total_size = total_size + blob.size   # Sum all of filtered blob objects size to get the total size
          count+=1                              # Count the number of files of filtered blob objects

    print("================================================")
    print("|         GCS Bucket Objects Statistic         |")
    print("================================================")
    print(" Total files: "+str(count)+" files")
    print(" Total size: "+str(total_size)+" bytes")
    print("================================================")
    print(" Mean: "+str(mean_size_selected_files(total_size, count)))
    print(" Min_size: "+str(format_bytes(min(size_only_blobs))))
    print(" Max_size: "+str(format_bytes(max(size_only_blobs))))
    print("=============== List of Files ==================")
    for blob in all_filtered_blobs:
        print(blob.name+": "+format_bytes(blob.size))
    print("================================================")