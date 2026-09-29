SELECT p.category, COUNT(*) AS total_count, COUNT(*) AS order_item_count
FROM order_items oi
JOIN products p ON p.id = oi.product_id
JOIN orders o ON oi.order_id = o.id
WHERE o.amount > 1110
GROUP BY p.category
ORDER BY p.category ASC
