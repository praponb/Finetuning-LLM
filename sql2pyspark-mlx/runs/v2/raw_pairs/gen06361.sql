select city, sum(rating) as total_rating, count(*) as cnt, sum(sum(rating)) over (order by city) as cumulative_rating from suppliers group by city;
