SELECT id, order_id, quantity FROM order_items WHERE product_id IN (11, 15, 25, 30) UNION SELECT id, order_id, quantity FROM order_items WHERE quantity != 6 ORDER BY order_id;
