SELECT p.brand, COUNT(oi.quantity) AS quantity_count, COUNT(*) AS cnt
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
LEFT JOIN orders o ON o.id = oi.order_id
WHERE p.stock_qty BETWEEN 152 AND 234
GROUP BY p.brand
ORDER BY cnt DESC, brand ASC
LIMIT 5
