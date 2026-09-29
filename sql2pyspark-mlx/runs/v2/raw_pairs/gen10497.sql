SELECT id, customer_id, channel FROM orders WHERE store_id IS NOT NULL UNION SELECT id, customer_id, channel FROM orders WHERE discount > 0.35;
