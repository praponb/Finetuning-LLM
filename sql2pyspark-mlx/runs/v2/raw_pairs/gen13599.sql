SELECT order_id, method, status, SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END) AS card_count FROM payments GROUP BY order_id, method, status;
