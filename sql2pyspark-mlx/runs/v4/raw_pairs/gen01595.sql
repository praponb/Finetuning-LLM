SELECT
    s.id,
    s.order_id,
    s.status,
    o.status AS order_status,
    o.amount
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
