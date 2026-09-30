SELECT c.id, c.segment
FROM customers c
WHERE c.segment != 'Consumer' AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);
