SELECT
    o.status,
    COUNT(oi.id) order_item_count,
    AVG(oi.unit_price) avg_unit_price,
    SUM(oi.unit_price * oi.quantity) revenue
FROM orders o
JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.status
HAVING AVG(oi.unit_price) >= 310
ORDER BY revenue DESC;
