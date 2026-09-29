select city, case when sqft >= 12332 then 'high' when sqft >= 3971 then 'mid' else 'low' end as sqft_bucket, round(stddev(sqft), 2) as stddev_sqft
from stores
group by city, sqft_bucket;
