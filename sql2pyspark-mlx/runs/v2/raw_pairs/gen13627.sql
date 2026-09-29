SELECT
    o.status,
    COUNT(oi.id) AS order_item_count,
    COUNT(DISTINCT oi.quantity) AS unique_orders,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM orders o
INNER JOIN order_items oi ON oi.order_id = o.id
WHERE o.shipping_fee <= 17
GROUP BY o.status
ORDER BY o.status
LIMIT 3;
