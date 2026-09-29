SELECT product_id, AVG(quantity) avg_quantity, SUM(quantity) total_quantity
FROM order_items
WHERE product_id IN (1, 22, 39) AND order_id IN (5, 22, 40)
GROUP BY product_id
HAVING AVG(quantity) >= 4.6
ORDER BY product_id DESC;
