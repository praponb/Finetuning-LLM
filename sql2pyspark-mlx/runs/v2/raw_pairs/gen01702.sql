SELECT o.id, o.shipping_fee, o.customer_id
FROM orders o
WHERE o.status NOT IN ('PENDING', 'COMPLETED') AND NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id);
