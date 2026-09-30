SELECT
    oi.id,
    oi.order_id,
    p.cost,
    p.product_name,
    o.discount,
    o.status
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
LEFT JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price > 160
ORDER BY product_name, order_id DESC
