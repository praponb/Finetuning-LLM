SELECT
    oi.id,
    oi.quantity,
    o.amount,
    o.channel
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
WHERE o.shipping_fee > 39
ORDER BY amount DESC, id
LIMIT 20;
