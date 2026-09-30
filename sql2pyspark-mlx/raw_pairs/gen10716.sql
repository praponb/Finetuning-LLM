SELECT
    orders.id,
    orders.amount,
    orders.customer_id,
    orders.order_date
FROM orders
WHERE NOT EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id);
