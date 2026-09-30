SELECT
    oi.quantity,
    oi.product_id,
    p.price,
    o.status,
    ARRAY_JOIN(p.tags, ', ') AS tags_text
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
LEFT JOIN orders o ON oi.order_id = o.id
ORDER BY oi.quantity DESC, oi.product_id DESC;
