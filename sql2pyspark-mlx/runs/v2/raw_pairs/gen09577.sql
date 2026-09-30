SELECT p.id, p.status FROM payments p WHERE p.amount >= 1480 AND p.order_id NOT IN (SELECT id FROM orders WHERE amount <= 980) ORDER BY p.status, id LIMIT 10
