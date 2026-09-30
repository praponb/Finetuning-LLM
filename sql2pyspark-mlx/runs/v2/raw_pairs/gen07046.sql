SELECT order_id, SUM(quantity) AS total_quantity, MAX(unit_price) AS max_unit_price FROM order_items WHERE (product_id = 13 AND order_id IN (1, 4, 17, 21)) OR product_id IS NULL GROUP BY order_id
