SELECT o.id, o.shipping_fee, o.store_id FROM orders o WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.order_id IN (6, 34, 39));
