SELECT p.brand, p.category, COUNT(*) AS cnt
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
FULL OUTER JOIN products p ON oi.product_id = p.id
GROUP BY p.brand, p.category
ORDER BY category, brand
LIMIT 10
