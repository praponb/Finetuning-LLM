SELECT o.id, o.customer_id FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE store_name IS NOT NULL) ORDER BY o.id DESC, o.customer_id DESC;
