select id, city, rating, round(avg(rating) over (partition by city order by rating, id rows between 6 preceding and current row), 2) as rating_moving_avg_7 from suppliers;
