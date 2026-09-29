SELECT
    s.id,
    s.cost,
    o.discount,
    o.channel,
    o.shipping_fee
FROM shipments s
RIGHT OUTER JOIN orders o ON o.id = s.order_id
WHERE s.order_id IS NULL
ORDER BY o.shipping_fee DESC;
