SELECT id, order_id, status FROM payments WHERE order_id = 34 EXCEPT SELECT id, order_id, status FROM payments WHERE order_id IN (22, 40) ORDER BY id;
