SELECT
    oi.id,
    oi.order_id,
    o.store_id,
    o.channel,
    p.category,
    UPPER(p.brand) AS brand_upper
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
LEFT JOIN products p ON oi.product_id = p.id
WHERE o.customer_id IS NULL
