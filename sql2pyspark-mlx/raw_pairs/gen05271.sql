SELECT c.id, c.email
FROM customers c
WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.customer_id IN (3, 15, 28, 39))
