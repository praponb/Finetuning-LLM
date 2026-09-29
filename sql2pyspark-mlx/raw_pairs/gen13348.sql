SELECT
    oi.id,
    oi.quantity,
    oi.order_id,
    p.brand,
    p.cost,
    o.status
FROM order_items oi
RIGHT JOIN products p ON oi.product_id = p.id
LEFT JOIN orders o ON o.id = oi.order_id
WHERE o.discount IS NOT NULL;
