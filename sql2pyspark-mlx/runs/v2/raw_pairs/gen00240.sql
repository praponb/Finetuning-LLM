SELECT product_id, SUM(quantity) AS total_quantity FROM order_items GROUP BY product_id ORDER BY total_quantity DESC, 1 DESC
