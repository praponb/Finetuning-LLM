SELECT
    s.id,
    s.order_id,
    s.carrier,
    o.channel,
    o.shipping_fee
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
