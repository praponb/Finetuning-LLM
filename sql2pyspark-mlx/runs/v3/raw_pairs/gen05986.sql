SELECT orders.id, orders.discount
FROM orders
WHERE EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id)
ORDER BY id DESC;
