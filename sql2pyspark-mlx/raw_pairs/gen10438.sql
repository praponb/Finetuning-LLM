SELECT id, order_id, SUBSTRING(status, 1, 3) status_prefix FROM payments ORDER BY order_id, id;
