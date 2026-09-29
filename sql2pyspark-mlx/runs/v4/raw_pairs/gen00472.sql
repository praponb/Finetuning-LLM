SELECT product_id, MAX(quantity) AS max_quantity, SUM(quantity) AS total_quantity
FROM order_items
WHERE product_id IN (7, 12, 23)
GROUP BY product_id
HAVING SUM(quantity) >= 18
