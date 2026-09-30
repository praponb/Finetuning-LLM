SELECT
    oi.id,
    oi.unit_price,
    o.order_date,
    o.store_id,
    COALESCE(oi.unit_price, 0) AS unit_price_filled
FROM order_items oi
LEFT JOIN orders o ON o.id = oi.order_id
WHERE oi.order_id IN (11, 17);
