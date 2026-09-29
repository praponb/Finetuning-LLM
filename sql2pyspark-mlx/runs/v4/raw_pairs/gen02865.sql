SELECT p.id, p.order_id, p.paid_at FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE status = 'PENDING') ORDER BY p.paid_at
