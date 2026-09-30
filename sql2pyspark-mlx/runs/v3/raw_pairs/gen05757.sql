SELECT id, country, city, rating, ROUND(AVG(rating) OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rating_moving_avg_7 FROM suppliers;
