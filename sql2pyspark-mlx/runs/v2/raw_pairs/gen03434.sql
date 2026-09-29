SELECT p.id, p.method FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE amount <= 1580)
