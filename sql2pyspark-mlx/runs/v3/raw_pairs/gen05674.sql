SELECT
    p.id,
    p.paid_at,
    o.order_date,
    o.amount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
WHERE o.status = 'COMPLETED' AND p.order_id IS NOT NULL
