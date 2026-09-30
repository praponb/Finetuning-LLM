SELECT o.id, o.shipping_fee, o.store_id
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.order_id IS NOT NULL);
