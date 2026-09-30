SELECT product_id, quantity FROM order_items WHERE product_id IN (15, 27, 32, 36) UNION ALL SELECT product_id, quantity FROM order_items WHERE order_id = 16;
