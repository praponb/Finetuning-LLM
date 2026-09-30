SELECT o.id, o.order_date, o.status, o.discount FROM orders o WHERE o.store_id IN (SELECT id FROM stores WHERE store_name IS NOT NULL)
