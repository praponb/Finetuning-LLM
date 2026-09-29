SELECT
    oi.id,
    oi.unit_price,
    p.cost,
    p.price,
    o.discount,
    o.store_id,
    TRIM(o.channel) AS clean_channel
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
WHERE p.launch_date < '2025-10-01' AND o.order_date >= '2024-12-08';
