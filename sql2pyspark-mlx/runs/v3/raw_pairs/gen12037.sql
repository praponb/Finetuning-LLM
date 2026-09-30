SELECT
    p.id,
    p.method,
    p.status,
    p.paid_at
FROM payments p
WHERE HOUR(p.paid_at) BETWEEN 8 AND 14 AND p.order_id IN (SELECT id FROM orders WHERE channel = 'partner');
