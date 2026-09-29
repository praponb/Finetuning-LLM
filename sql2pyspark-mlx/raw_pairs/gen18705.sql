SELECT p.id, p.status, p.amount FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE channel IS NULL)
