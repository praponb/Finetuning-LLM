select country, city, count(case when city = 'New York' then 1 end) as new_york_count, min(rating) as min_rating from suppliers group by country, city
