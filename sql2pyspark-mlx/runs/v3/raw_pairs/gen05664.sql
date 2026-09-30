SELECT p.id, p.paid_at, p.status FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE amount IS NOT NULL);
