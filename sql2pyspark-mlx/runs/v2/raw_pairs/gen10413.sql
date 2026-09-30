select * from (select country, city, rating from suppliers)
pivot (sum(rating) for city in ('Bangkok', 'Chiang Mai'))
