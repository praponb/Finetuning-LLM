SELECT order_id, MAX(amount) AS max_amount, SUM(amount) AS total_amount, MAX(amount) - MIN(amount) AS amount_range FROM payments GROUP BY order_id ORDER BY 2 DESC;
