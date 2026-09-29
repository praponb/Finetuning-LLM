SELECT p.id, COUNT(oi.id) AS order_item_count, SUM(oi.unit_price) AS total_unit_price
FROM products p
LEFT JOIN order_items oi ON oi.product_id = p.id
WHERE oi.quantity >= 7
GROUP BY p.id
HAVING COUNT(oi.id) >= 1
ORDER BY total_unit_price, id
LIMIT 1;
