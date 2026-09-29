SELECT p.id, p.status FROM payments p WHERE p.amount < 650 AND p.order_id NOT IN (SELECT id FROM orders WHERE customer_id IS NULL) ORDER BY status, id LIMIT 3;
