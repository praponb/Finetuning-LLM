SELECT
    oi.id,
    oi.quantity,
    oi.order_id,
    oi.product_id,
    p.brand
FROM order_items oi
JOIN products p ON p.id = oi.product_id
ORDER BY quantity DESC;
