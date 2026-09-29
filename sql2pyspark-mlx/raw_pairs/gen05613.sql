SELECT p.category, COUNT(oi.quantity) AS quantity_count
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
WHERE p.brand IN ('Globex', 'Stark')
GROUP BY p.category
ORDER BY p.category ASC
LIMIT 1
