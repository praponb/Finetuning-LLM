SELECT product_id, quantity FROM order_items WHERE order_id IN (20, 21, 26, 28) UNION SELECT product_id, quantity FROM order_items WHERE quantity = 6;
