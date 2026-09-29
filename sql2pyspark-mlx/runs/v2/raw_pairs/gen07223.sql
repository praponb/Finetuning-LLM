SELECT
    oi.order_id,
    oi.product_id,
    oi.unit_price,
    p.brand
FROM order_items oi
JOIN products p ON oi.product_id = p.id
ORDER BY order_id DESC;
