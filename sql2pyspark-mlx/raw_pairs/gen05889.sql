SELECT order_id, SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count, COUNT(amount) AS amount_count FROM payments GROUP BY order_id HAVING success_count >= 40
