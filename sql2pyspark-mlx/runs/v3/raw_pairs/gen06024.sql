SELECT
    o.store_id,
    o.status,
    p.method,
    p.amount
FROM orders o
JOIN payments p ON p.order_id = o.id
WHERE p.order_id = 1
ORDER BY method
