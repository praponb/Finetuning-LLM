SELECT o.id, o.store_id, o.status, o.amount FROM orders o WHERE o.store_id IN (SELECT id FROM stores WHERE store_name IS NOT NULL)
