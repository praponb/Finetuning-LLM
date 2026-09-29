SELECT
    oi.id,
    oi.unit_price,
    o.discount,
    o.order_date
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
WHERE o.discount IS NULL AND oi.product_id = 17
