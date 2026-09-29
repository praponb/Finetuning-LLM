SELECT
    oi.id,
    oi.quantity,
    o.shipping_fee,
    o.status
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE o.shipping_fee <= 26 AND o.channel = 'store'
ORDER BY shipping_fee DESC;
