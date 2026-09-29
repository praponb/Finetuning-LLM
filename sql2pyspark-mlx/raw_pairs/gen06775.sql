SELECT
    p.id,
    p.method,
    p.paid_at,
    p.status,
    o.shipping_fee,
    o.customer_id
FROM payments p
JOIN orders o ON p.order_id = o.id
ORDER BY status, id
LIMIT 3
