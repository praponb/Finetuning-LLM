SELECT
    o.store_id,
    p.order_id,
    p.paid_at,
    p.amount
FROM orders o
INNER JOIN payments p ON p.order_id = o.id
WHERE p.paid_at >= '2024-07-03'
ORDER BY o.store_id ASC
