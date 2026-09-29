SELECT p.id, p.method FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE store_id = 40)
