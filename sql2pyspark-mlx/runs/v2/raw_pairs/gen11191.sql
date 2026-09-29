SELECT product_id, order_id, MIN(quantity) AS min_quantity FROM order_items GROUP BY 1, 2 ORDER BY order_id DESC
