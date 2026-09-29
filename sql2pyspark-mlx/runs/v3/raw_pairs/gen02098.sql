select * from (select city, country, rating from suppliers)
pivot (avg(rating) for country in ('SG', 'US', 'DE'))
order by city
