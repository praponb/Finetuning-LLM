SELECT
    p.id,
    p.amount,
    p.paid_at,
    p.method
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE store_id IS NOT NULL)
ORDER BY id;
