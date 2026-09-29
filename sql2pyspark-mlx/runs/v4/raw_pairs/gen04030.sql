SELECT p.id, p.method, p.paid_at, p.amount FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE discount < 0.11) ORDER BY method, paid_at;
