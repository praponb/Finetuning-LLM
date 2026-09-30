SELECT
    p.id,
    p.method,
    p.amount,
    p.order_id
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee < 17)
ORDER BY p.id DESC, amount DESC
LIMIT 1
