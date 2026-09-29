SELECT id, city, rating, ROUND(AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rating_moving_avg_3 FROM suppliers ORDER BY rating_moving_avg_3
