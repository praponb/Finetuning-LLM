SELECT id, country, rating, LAG(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) AS prev_rating FROM suppliers ORDER BY prev_rating, 3
