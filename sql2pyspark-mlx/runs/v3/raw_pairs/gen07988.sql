SELECT o.id, o.status, o.shipping_fee
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id);
