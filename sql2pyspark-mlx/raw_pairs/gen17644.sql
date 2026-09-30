SELECT p.id, p.order_id FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE customer_id IS NULL) ORDER BY p.id DESC;
