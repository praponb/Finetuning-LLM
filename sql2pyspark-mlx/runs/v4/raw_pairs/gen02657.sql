SELECT c.id, c.city, c.name
FROM customers c
WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id);
