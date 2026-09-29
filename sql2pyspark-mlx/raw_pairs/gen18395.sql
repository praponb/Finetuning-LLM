select id, country, city, rating, percent_rank() over (partition by country, city order by rating desc) as rating_pct_rank from suppliers;
