SELECT
    p.id,
    p.method,
    o.customer_id,
    o.channel,
    o.store_id,
    CONCAT(o.channel, ', ', o.status) channel_status
FROM payments p
JOIN orders o ON o.id = p.order_id
WHERE o.amount < 1210;
