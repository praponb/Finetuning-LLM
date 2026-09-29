SELECT
    oi.id,
    oi.quantity,
    p.price,
    p.launch_date,
    o.order_date,
    o.discount
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
LEFT JOIN orders o ON oi.order_id = o.id
WHERE o.store_id IN (1, 8)
ORDER BY o.discount DESC;
