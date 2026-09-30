SELECT id, quantity, order_id FROM order_items WHERE order_id = 3 UNION ALL SELECT id, quantity, order_id FROM order_items WHERE order_id = 10;
