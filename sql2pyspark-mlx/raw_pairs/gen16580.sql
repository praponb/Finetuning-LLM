SELECT
    oi.id,
    oi.product_id,
    oi.unit_price,
    o.order_date,
    o.channel,
    s.opened_date,
    s.region
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id
WHERE o.shipping_fee >= 13 AND oi.product_id IN (16, 26, 27, 34)
ORDER BY o.channel DESC;
