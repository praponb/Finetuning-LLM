SELECT id, rating, ROUND(AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rating_moving_avg_7 FROM suppliers ORDER BY rating DESC, id DESC;
