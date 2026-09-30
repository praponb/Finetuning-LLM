SELECT o.id, o.customer_id, o.store_id, o.channel FROM orders o WHERE o.customer_id NOT IN (SELECT id FROM customers WHERE name IS NOT NULL) ORDER BY id DESC
