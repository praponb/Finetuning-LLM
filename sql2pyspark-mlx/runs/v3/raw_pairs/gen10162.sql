SELECT p.id, p.order_id, p.method FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE customer_id IN (16, 24, 25, 40))
