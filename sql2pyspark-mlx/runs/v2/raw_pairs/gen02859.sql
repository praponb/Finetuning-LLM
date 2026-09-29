SELECT
    p.id,
    p.amount,
    p.paid_at,
    o.store_id
FROM payments p
JOIN orders o ON p.order_id = o.id
WHERE p.status IN ('SUCCESS', 'REFUNDED', 'FAILED') AND o.store_id IN (27, 28, 37, 39)
ORDER BY p.id ASC;
