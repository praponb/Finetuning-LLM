SELECT p.category, AVG(oi.quantity) AS avg_quantity
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE oi.product_id IS NULL
GROUP BY p.category
ORDER BY avg_quantity DESC, p.category;
