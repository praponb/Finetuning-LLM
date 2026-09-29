SELECT id, rating, ROUND(AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS rating_moving_avg_4 FROM suppliers
