SELECT id, order_id FROM order_items WHERE order_id = 20 UNION ALL SELECT id, order_id FROM order_items WHERE product_id IN (4, 10, 33, 38)
