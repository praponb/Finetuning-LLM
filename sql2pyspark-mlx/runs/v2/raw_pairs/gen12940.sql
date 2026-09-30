select * from (select year(opened_date) as opened_year, city, sqft from stores)
pivot (count(sqft) for city in ('Bangkok', 'Chiang Mai'));
