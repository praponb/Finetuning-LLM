SELECT
    p.id,
    p.paid_at,
    p.method,
    o.status,
    o.customer_id,
    o.channel
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
ORDER BY o.status, customer_id DESC, id
LIMIT 5
