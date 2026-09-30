select city, avg(rating) as avg_rating, sum(case when city = 'London' then 1 else 0 end) as london_count from suppliers where rating > 2.2 group by city
