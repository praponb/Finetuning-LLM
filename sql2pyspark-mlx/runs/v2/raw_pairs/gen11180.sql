SELECT product_id, order_id, COUNT(quantity) AS quantity_count
FROM order_items
WHERE order_id IN (10, 24, 37, 40) AND quantity < 7
GROUP BY product_id, order_id
HAVING COUNT(quantity) < 2
ORDER BY order_id DESC;
