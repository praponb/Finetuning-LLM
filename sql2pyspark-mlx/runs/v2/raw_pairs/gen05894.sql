SELECT order_id, COUNT(*) AS total_count FROM order_items WHERE quantity > 5 AND unit_price IS NULL GROUP BY 1 HAVING total_count > 4 ORDER BY 1 DESC;
