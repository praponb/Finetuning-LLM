SELECT
    p.id,
    p.status,
    p.order_id,
    p.method
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE shipping_fee >= 28)
ORDER BY method, id
LIMIT 1;
