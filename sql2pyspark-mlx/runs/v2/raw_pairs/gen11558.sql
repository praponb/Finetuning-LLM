SELECT p.id, p.method, p.order_id, p.amount FROM payments p WHERE p.amount <= 980 AND p.order_id NOT IN (SELECT id FROM orders WHERE status = 'PENDING') ORDER BY method, order_id DESC
