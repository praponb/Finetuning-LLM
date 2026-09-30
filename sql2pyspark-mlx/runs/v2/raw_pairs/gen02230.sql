select * from (select region, city, sqft from stores)
pivot (count(sqft) for city in ('New York', 'London', 'Bangkok'));
