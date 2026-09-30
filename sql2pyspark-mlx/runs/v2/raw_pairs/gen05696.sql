select id, country, rating, max(rating) over (partition by country) as country_max_rating from suppliers
