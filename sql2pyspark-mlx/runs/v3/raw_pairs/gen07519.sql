SELECT o.id, o.store_id FROM orders o WHERE o.shipping_fee <= 26 AND NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.order_id IS NOT NULL)
