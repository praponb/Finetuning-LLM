select region, city, round(stddev(sqft), 2) stddev_sqft from stores group by region, city order by city desc
