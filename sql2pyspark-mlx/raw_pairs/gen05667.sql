select * from (select city, region, sqft from stores where city in ('Singapore', 'Tokyo')) pivot (avg(sqft) for region in ('Central', 'South', 'West'))
