select * from (select city, region, sqft from stores)
pivot (count(sqft) for region in ('South', 'West', 'East'))
order by city
