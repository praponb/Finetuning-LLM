SELECT id, country, city, rating, ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 3) AS rating_share FROM suppliers;
