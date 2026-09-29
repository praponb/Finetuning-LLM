SELECT
    p.id,
    p.order_id,
    p.amount,
    p.method
FROM payments p
WHERE p.method IS NOT NULL AND p.order_id IN (SELECT id FROM orders WHERE discount < 0.15);
