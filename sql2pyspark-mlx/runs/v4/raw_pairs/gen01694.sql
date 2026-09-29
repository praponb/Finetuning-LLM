SELECT
    p.id,
    p.amount,
    p.status,
    p.method
FROM payments p
WHERE TO_DATE(p.paid_at) < '2023-04-01' AND p.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee >= 13)
ORDER BY p.amount DESC;
