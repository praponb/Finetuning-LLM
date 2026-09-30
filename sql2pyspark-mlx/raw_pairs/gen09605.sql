SELECT
    p.id,
    p.paid_at,
    p.amount,
    p.status
FROM payments p
WHERE p.paid_at < '2024-10-01' AND p.order_id IN (SELECT id FROM orders WHERE status IN ('COMPLETED', 'CANCELLED', 'PENDING'))
