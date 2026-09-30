SELECT o.id, o.store_id, o.shipping_fee, o.order_date FROM orders o WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.order_id IN (3, 4, 22))
