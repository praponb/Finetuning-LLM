SELECT
    oi.id,
    oi.unit_price,
    oi.order_id,
    o.channel,
    o.amount
FROM order_items oi
RIGHT OUTER JOIN orders o ON oi.order_id = o.id
WHERE o.status = 'CANCELLED';
