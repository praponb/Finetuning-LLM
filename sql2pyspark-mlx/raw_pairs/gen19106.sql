SELECT id, order_id FROM order_items EXCEPT DISTINCT SELECT id, order_id FROM order_items WHERE order_id = 26;
