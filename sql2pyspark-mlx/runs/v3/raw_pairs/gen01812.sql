select id, city, rating, avg(rating) over (partition by city) as city_avg_rating from suppliers order by 4 desc
