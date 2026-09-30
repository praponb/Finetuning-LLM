select * from (select country, city, rating from suppliers) src pivot (sum(rating) as value_sum, count(1) as item_count for city in ('Tokyo', 'Singapore'))
