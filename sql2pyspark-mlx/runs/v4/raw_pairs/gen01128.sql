SELECT o.id, o.store_id, o.status, o.shipping_fee FROM orders o WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.order_id IN (12, 19, 22, 34)) ORDER BY o.store_id
