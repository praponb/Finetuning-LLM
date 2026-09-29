select id, country, city, rating, min(rating) over (partition by country, city) as country_min_rating from suppliers;
