SELECT *, MIN(rating) OVER (PARTITION BY city) AS city_min_rating, ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 4) AS rating_share FROM suppliers
