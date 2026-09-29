SELECT id, order_id, quantity FROM order_items WHERE order_id IN (10, 11, 17, 26) ORDER BY 3 DESC, id DESC LIMIT 1;
