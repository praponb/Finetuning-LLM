SELECT product_id, MAX(quantity) max_quantity, AVG(quantity) avg_quantity
FROM order_items
GROUP BY product_id
HAVING AVG(quantity) >= 7.4
