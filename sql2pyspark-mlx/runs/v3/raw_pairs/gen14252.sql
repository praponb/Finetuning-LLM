SELECT
    o.status,
    o.channel,
    o.store_id,
    s.carrier,
    s.cost,
    s.order_id
FROM orders o
INNER JOIN shipments s ON o.id = s.order_id
ORDER BY order_id DESC, store_id DESC;
