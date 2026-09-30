SELECT
    o.id,
    COUNT(oi.id) AS order_item_count,
    ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity,
    COUNT(oi.unit_price) AS unit_price_count
FROM orders o
INNER JOIN order_items oi ON oi.order_id = o.id
WHERE oi.quantity <= 7
GROUP BY o.id
ORDER BY order_item_count DESC;
