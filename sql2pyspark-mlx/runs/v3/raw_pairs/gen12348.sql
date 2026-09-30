SELECT o.id, o.discount, o.status FROM orders o WHERE o.store_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id)
