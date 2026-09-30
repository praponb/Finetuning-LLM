SELECT id, country, rating, ROUND(AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) rating_moving_avg_3 FROM suppliers ORDER BY id DESC
