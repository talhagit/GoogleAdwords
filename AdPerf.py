#!/usr/bin/env python
#
# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""

The LoadFromStorage method is pulling credentials and properties from a
"googleads.yaml" file. By default, it looks for this file in your home
directory.
"""


from googleads import adwords
import _locale
import pandas as pd
import io
from datetime import datetime
_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8']) # change encoding


def main(client):
  # Initialize  service.
  report_downloader = client.GetReportDownloader(version='v201809') 
  output= io.StringIO()
  # Create report query.
  report_query = (adwords.ReportQueryBuilder()
                  .Select("AccountDescriptiveName","CampaignId","AdGroupId","Id","Headline","HeadlinePart1","HeadlinePart2","ShortHeadline","LongHeadline","CreativeFinalUrls","ImageCreativeName","Description","Description1","Description2","DisplayUrl","Path1","Path2","BusinessName","Status","AdGroupStatus","CampaignStatus","CombinedApprovalStatus","AdType","Labels","Impressions","Interactions","InteractionRate","AverageCost",
                          "Cost","VideoQuartile100Rate","Clicks","AveragePosition","Conversions","Date")
                  .From('AD_PERFORMANCE_REPORT')#startdate,enddate missing
                  #.Where('Campaign_status').In('ENABLED', 'PAUSED')
                  .During('LAST_7_DAYS')
                  .Build())
  #print(report_query)
  # You can provide a file object to write the output to..
  #output= io.StringIO()
  report_downloader.DownloadReportWithAwql(
      report_query,'TSV',output, skip_report_header=True, #tab delimited
      skip_column_header=True, skip_report_summary=False,
      include_zero_impressions=True)
  
  output.seek(0)
  
  types= {'Cost':pd.np.float64,'Conversions': pd.np.str ,'Avg.Cost': pd.np.float64} # Change datatype.

  cols=["Account","Campaign ID","Ad group ID","Ad ID","Headline","Headline 1","Headline 2" ,"Short headline","Long headline","Ad final URL","Image ad name","Description","Description 1","Description 2","Display URL",
        "Path 1","Path 2","Business name","Ad status","Adgroup Status","Campaign Status","CombinedApprovalStatus","Ad type","Labels on Ad","Impressions","Interactions","InteractionRate","Avg. Cost","Cost"
        ,"Video played to 100%","Clicks","Avg. position","Conversions","Date"]
  
  df = pd.read_csv(output,dtype=types,sep="\t",low_memory=False, na_values=[' --'],names=cols)
 # print(df.head())
  df['Cost']=df.Cost/1000000
  df['Avg. Cost']= df['Avg. Cost']/1000000
  df['Ad']=df.Headline
  df.drop(df.tail(1).index,inplace=True) # drop footer
  df.to_csv('AdPerformaceReport-%s.csv'%datetime.now().strftime('%Y%m%d%H%M%S'),index=False,sep="\t") # export to default working directory
  
  
if __name__ == '__main__':
  # Initialize client object.
  adwords_client = adwords.AdWordsClient.LoadFromStorage() # Config file from default location
  
  main(adwords_client)