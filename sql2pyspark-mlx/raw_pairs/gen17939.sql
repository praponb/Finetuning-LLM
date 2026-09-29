SELECT id, store_id FROM orders WHERE store_id IS NULL UNION SELECT id, store_id FROM orders WHERE status != 'SHIPPED';
