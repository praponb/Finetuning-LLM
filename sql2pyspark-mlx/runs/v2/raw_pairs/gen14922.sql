select id, country, rating, avg(rating) over (partition by country) as country_avg_rating from suppliers order by country_avg_rating desc
