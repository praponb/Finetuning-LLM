select * from (select city, region, sqft from stores) pivot (avg(sqft) for region in ('South', 'West'));
