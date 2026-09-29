SELECT
    o.id,
    o.status,
    o.channel,
    o.shipping_fee
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.order_id IN (23, 24, 33))
