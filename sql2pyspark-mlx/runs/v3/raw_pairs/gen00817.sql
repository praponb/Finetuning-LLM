SELECT id, country, rating, ROUND(rating / SUM(rating) OVER (PARTITION BY country), 3) rating_share FROM suppliers;
