SELECT
    oi.id,
    oi.unit_price,
    oi.quantity,
    p.brand,
    o.status
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
LEFT JOIN orders o ON oi.order_id = o.id
WHERE SIZE(p.tags) >= 2 OR o.channel = 'partner'
