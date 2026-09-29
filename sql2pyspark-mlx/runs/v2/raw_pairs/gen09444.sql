SELECT
    o.id,
    o.status,
    COUNT(oi.id) order_item_count,
    MAX(oi.unit_price) max_unit_price
FROM orders o
LEFT JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.id, o.status
ORDER BY order_item_count DESC;
