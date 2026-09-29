SELECT product_id, MAX(quantity) AS max_quantity FROM order_items GROUP BY product_id ORDER BY product_id DESC LIMIT 5;
