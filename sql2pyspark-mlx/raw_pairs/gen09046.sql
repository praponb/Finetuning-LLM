select * from (select year(opened_date) as opened_year, city, sqft from stores)
pivot (sum(sqft) for city in ('Chiang Mai', 'Tokyo', 'London'));
