SELECT
    oi.id,
    oi.quantity,
    o.status,
    o.channel,
    s.region
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
JOIN stores s ON o.store_id = s.id
WHERE o.shipping_fee BETWEEN 19 AND 20;
