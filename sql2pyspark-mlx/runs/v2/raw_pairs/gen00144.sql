select case when sqft >= 16129 then 'high' when sqft >= 2491 then 'mid' else 'low' end as sqft_bucket, round(stddev(sqft), 2) as stddev_sqft from stores group by 1;
