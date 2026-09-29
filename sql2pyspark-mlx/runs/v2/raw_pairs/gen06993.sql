SELECT
    oi.id,
    oi.order_id,
    oi.unit_price,
    oi.quantity
FROM order_items oi
WHERE oi.order_id IN (SELECT id FROM orders WHERE status IN ('PENDING', 'COMPLETED', 'CANCELLED'))
