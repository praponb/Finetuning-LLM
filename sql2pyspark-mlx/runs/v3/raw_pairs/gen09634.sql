SELECT
    o.shipping_fee,
    o.store_id,
    o.channel,
    s.status,
    s.carrier
FROM orders o
LEFT JOIN shipments s ON o.id = s.order_id
ORDER BY s.carrier;
