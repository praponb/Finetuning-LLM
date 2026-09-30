select * from (select city, region, sqft from stores)
pivot (sum(sqft) for region in ('South', 'Central', 'East', 'North'))
order by city
