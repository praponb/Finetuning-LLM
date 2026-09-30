SELECT
    o.store_id,
    o.channel,
    o.shipping_fee,
    p.amount,
    p.paid_at
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id
WHERE o.amount >= 810
