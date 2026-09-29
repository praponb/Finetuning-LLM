SELECT
    oi.id,
    oi.order_id,
    oi.product_id,
    oi.unit_price,
    o.status,
    o.store_id
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
WHERE o.store_id IS NOT NULL
ORDER BY id ASC;
