SELECT p.id, p.order_id, p.paid_at, p.status FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE amount < 1410) ORDER BY status, paid_at
