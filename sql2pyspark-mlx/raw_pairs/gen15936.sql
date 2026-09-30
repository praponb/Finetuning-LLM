SELECT p.id, p.status, p.paid_at FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE amount < 490)
