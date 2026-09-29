SELECT orders.id, orders.amount, orders.shipping_fee
FROM orders
WHERE EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id AND shipments.delivered_date >= '2025-05-01')
ORDER BY orders.shipping_fee DESC, id DESC
