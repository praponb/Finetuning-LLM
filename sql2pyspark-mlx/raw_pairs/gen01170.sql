SELECT
    p.id,
    p.order_id,
    p.paid_at,
    o.discount,
    o.amount,
    o.shipping_fee
FROM payments p
JOIN orders o ON o.id = p.order_id
WHERE p.order_id = 14;
