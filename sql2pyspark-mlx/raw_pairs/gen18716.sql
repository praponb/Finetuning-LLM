SELECT orders.id, orders.status
FROM orders
WHERE orders.shipping_fee > 23 AND NOT EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id);
