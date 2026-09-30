SELECT method, order_id, COUNT(*) num_payments FROM payments GROUP BY method, order_id ORDER BY order_id
