SELECT p.category, COUNT(DISTINCT oi.unit_price) AS unique_orders, COUNT(*) AS num_order_items
FROM order_items oi
INNER JOIN orders o ON o.id = oi.order_id
INNER JOIN products p ON oi.product_id = p.id
GROUP BY p.category
HAVING COUNT(DISTINCT oi.unit_price) >= 4
ORDER BY num_order_items DESC;
