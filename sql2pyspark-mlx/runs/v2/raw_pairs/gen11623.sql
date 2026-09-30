SELECT
    p.id,
    p.status,
    p.paid_at,
    p.order_id
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE store_id IN (10, 15))
ORDER BY p.order_id DESC;
