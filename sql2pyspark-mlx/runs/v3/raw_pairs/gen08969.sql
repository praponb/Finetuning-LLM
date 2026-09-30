SELECT
    oi.id,
    oi.unit_price,
    p.launch_date,
    o.amount
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
WHERE ARRAY_CONTAINS(p.tags, 'new');
