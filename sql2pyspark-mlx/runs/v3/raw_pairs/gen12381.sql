SELECT
    p.method,
    p.order_id,
    p.paid_at,
    o.discount
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
WHERE o.status IN ('CANCELLED', 'PENDING');
