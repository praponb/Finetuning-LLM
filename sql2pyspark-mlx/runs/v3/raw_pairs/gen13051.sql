SELECT id, quantity FROM order_items WHERE quantity < 8 EXCEPT SELECT id, quantity FROM order_items WHERE product_id IN (1, 31, 36) ORDER BY id;
