SELECT order_id, COUNT(DISTINCT quantity) AS unique_orders, MIN(quantity) AS min_quantity FROM order_items WHERE product_id = 8 GROUP BY order_id
