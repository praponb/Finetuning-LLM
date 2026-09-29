SELECT
    s.id,
    s.order_id,
    o.amount,
    o.shipping_fee
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE o.status IN ('RETURNED', 'PENDING', 'CANCELLED')
ORDER BY id
