SELECT
    o.status,
    COUNT(oi.id) AS order_item_count,
    AVG(oi.unit_price) AS avg_unit_price,
    MIN(oi.unit_price) AS min_unit_price
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.id
GROUP BY o.status
