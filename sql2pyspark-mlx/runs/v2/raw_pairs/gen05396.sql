SELECT order_id, COUNT(*) AS total_count, ROUND(AVG(quantity), 2) AS avg_quantity, SUM(quantity) AS total_quantity FROM order_items GROUP BY order_id HAVING total_quantity > 33
