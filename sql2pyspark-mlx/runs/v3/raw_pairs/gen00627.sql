SELECT
    p.id,
    p.status,
    p.amount,
    p.method
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE order_date < '2024-01-01')
ORDER BY p.id
LIMIT 50;
