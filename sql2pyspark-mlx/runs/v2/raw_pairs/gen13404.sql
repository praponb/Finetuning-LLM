SELECT c.id, c.email
FROM customers c
WHERE c.is_active = FALSE AND c.id IN (SELECT customer_id FROM orders);
