SELECT id, order_id, quantity, MAX(quantity) OVER (PARTITION BY order_id) AS order_id_max_quantity FROM order_items WHERE product_id IN (10, 30, 33) ORDER BY order_id DESC;
