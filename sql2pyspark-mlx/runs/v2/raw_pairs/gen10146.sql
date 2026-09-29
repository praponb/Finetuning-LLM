SELECT
    p.id,
    p.status,
    p.paid_at,
    p.method
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE channel = 'partner')
ORDER BY paid_at, id
LIMIT 20;
