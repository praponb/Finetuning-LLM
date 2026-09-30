select * from (select country, city, rating from suppliers)
pivot (count(rating) for city in ('Chiang Mai', 'Berlin', 'Bangkok', 'London'));
