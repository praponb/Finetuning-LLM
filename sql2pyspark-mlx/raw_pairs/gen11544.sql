SELECT
    p.id,
    p.paid_at,
    p.method,
    p.order_id
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE status <> 'COMPLETED')
