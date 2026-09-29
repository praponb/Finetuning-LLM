SELECT p.id, p.status, p.method, p.paid_at FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE customer_id IN (4, 25, 32))
