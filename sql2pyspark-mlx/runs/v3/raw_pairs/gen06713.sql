SELECT
    p.id,
    p.order_id,
    p.status,
    o.channel
FROM payments p
INNER JOIN orders o ON o.id = p.order_id;
