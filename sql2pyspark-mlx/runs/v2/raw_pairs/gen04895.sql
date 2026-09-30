SELECT p.id, p.status, p.amount, p.method FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE status = 'PENDING');
