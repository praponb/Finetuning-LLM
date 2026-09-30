SELECT status FROM orders WHERE store_id IN (35, 36) UNION SELECT status FROM orders WHERE status = 'CANCELLED';
