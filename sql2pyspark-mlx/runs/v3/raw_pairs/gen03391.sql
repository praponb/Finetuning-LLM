select * from (select country, city, rating from suppliers where rating >= 2.2)
pivot (sum(rating) for city in ('Chiang Mai', 'Tokyo', 'New York'))
order by country
