SELECT
    oi.id,
    oi.quantity,
    o.amount,
    o.shipping_fee,
    o.status
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
ORDER BY shipping_fee DESC;
