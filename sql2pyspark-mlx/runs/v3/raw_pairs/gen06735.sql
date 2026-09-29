SELECT
    o.id,
    o.channel,
    o.customer_id,
    o.shipping_fee
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.quantity BETWEEN 7 AND 8)
ORDER BY o.id;
