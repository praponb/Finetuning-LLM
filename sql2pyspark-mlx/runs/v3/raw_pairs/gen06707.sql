SELECT
    oi.id,
    oi.order_id,
    oi.product_id,
    oi.unit_price,
    p.price
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
WHERE oi.product_id IS NOT NULL
ORDER BY 5;
