SELECT
    p.id,
    p.paid_at,
    p.status,
    p.amount
FROM payments p
WHERE p.amount BETWEEN 480 AND 1500 AND p.order_id NOT IN (SELECT id FROM orders WHERE channel IN ('partner', 'web', 'store'))
