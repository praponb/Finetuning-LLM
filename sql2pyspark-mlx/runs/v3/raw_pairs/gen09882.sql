SELECT
    oi.id,
    oi.unit_price,
    p.category,
    p.product_name,
    o.channel
FROM order_items oi
RIGHT JOIN products p ON p.id = oi.product_id
JOIN orders o ON oi.order_id = o.id
WHERE ARRAY_CONTAINS(p.tags, 'new');
