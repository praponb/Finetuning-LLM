select * from (select region, city, sqft from stores) pivot (avg(sqft) for city in ('Bangkok', 'New York'))
