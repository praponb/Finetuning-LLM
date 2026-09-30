SELECT status, order_id, MIN(amount) AS min_amount, AVG(amount) AS avg_amount, MAX(amount) AS max_amount FROM payments GROUP BY status, order_id
