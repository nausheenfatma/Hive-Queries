#!/bin/bash

#businessreview table

hive -e "ADD JAR ../bin/json-serde.jar; 
CREATE EXTERNAL TABLE IF NOT EXISTS businessreview (   
votes STRUCT<cool:INT, funny:INT, useful:INT>, 
user_id STRING,
review_id STRING,
stars INT,
date STRING,
text STRING,
type STRING,
business_id STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde';
 
LOAD DATA LOCAL INPATH '../../test_cases/business_review.txt' INTO TABLE businessreview;
 
CREATE EXTERNAL TABLE IF NOT EXISTS businessdetails(
business_id STRING,
full_address STRING,
hours MAP<STRING, MAP<STRING,TIMESTAMP>>,
open BOOLEAN,
categories ARRAY<STRING>,
city STRING,
review_count STRING,
name STRING,
neighbourhoods ARRAY<STRING>,
longitude DOUBLE,
state STRING,
stars FLOAT,
latitude DOUBLE,
attributes MAP<STRING,MAP<STRING,BOOLEAN>>,
type BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde';

LOAD DATA LOCAL INPATH '../../test_cases/business_details.txt' INTO TABLE businessdetails;


select col1,count(distinct b.business_id1) 
from (select split(city,', ') as city1,business_id as business_id1 from businessdetails) as b
Lateral VIEW explode(b.city1) mytable as col1 
group by col1;



SELECT  b.monthname, b.count from (
SELECT  count(distinct review_id) as count,month(date) as month,
CASE
WHEN month(date)=1 THEN 'January'
WHEN month(date)=2 THEN 'February'
WHEN month(date)=3 THEN 'March'
WHEN month(date)=4 THEN 'April'
WHEN month(date)=5 THEN 'May'
WHEN month(date)=6 THEN 'June'
WHEN month(date)=7 THEN 'July'
WHEN month(date)=8 THEN 'August'
WHEN month(date)=9 THEN 'September'
WHEN month(date)=10 THEN 'October'
WHEN month(date)=11 THEN 'November'
WHEN month(date)=12 THEN 'December'
END AS monthname
from businessreview 
group by month(date) order by month) as b;


select bt.name,collect_set(br.user_id) from businessreview br left join businessdetails bt on (br.business_id=bt.business_id) where br.stars=5 and br.user_id is not null group by br.business_id,bt.name order by bt.name;
"
