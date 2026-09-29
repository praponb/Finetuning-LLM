SELECT
    p.id,
    p.order_id,
    p.paid_at,
    p.amount
FROM payments p
WHERE p.method = 'promptpay' AND p.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee BETWEEN 20 AND 24)
