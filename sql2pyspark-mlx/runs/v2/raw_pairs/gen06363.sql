SELECT o.id, o.store_id FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE LENGTH(store_name) < 10)
