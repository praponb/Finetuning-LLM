SELECT p.category, MIN(oi.unit_price) AS min_unit_price
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON o.id = oi.order_id
GROUP BY p.category
ORDER BY category
