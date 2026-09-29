SELECT id, country, rating, ROUND(AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS rating_moving_avg_4 FROM suppliers;
