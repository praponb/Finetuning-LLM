SELECT customer_id, COUNT(*) AS total_count FROM orders WHERE amount BETWEEN 1150 AND 1240 OR discount >= 0.14 GROUP BY customer_id ORDER BY customer_id DESC;
