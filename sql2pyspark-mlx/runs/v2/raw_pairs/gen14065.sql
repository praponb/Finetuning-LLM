SELECT p.id, p.paid_at FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE amount BETWEEN 640 AND 1060) ORDER BY p.paid_at, id LIMIT 1;
