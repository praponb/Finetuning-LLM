SELECT p.id, p.method, p.order_id FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE store_id IN (11, 19, 33, 40));
