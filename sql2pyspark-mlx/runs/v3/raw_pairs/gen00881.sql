SELECT product_id FROM order_items WHERE order_id IN (17, 30, 33, 38) UNION ALL SELECT product_id FROM order_items WHERE quantity < 3;
