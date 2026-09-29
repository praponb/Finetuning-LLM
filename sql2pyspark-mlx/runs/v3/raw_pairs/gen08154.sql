SELECT
    oi.id,
    oi.unit_price,
    oi.order_id,
    o.shipping_fee,
    s.opened_date,
    s.store_name
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN stores s ON s.id = o.store_id
ORDER BY unit_price DESC;
