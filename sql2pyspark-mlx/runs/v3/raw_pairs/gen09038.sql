select * from (select city, region, sqft from stores)
pivot (sum(sqft) for region in ('South', 'East', 'West'))
order by city
