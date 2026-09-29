select * from (select city, region, sqft from stores) pivot (avg(sqft) for region in ('Central', 'East', 'South', 'North')) order by city
