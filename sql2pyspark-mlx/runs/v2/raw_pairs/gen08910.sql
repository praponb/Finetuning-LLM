SELECT
    p.id,
    p.status,
    p.order_id,
    p.method
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE status IN ('PENDING', 'RETURNED', 'SHIPPED'))
ORDER BY p.id DESC;
