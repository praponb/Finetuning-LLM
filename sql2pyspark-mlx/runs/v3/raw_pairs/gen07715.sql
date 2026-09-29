SELECT
    p.id,
    p.paid_at,
    p.amount,
    p.method
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE channel NOT IN ('web', 'partner', 'store'));
