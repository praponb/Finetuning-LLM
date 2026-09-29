SELECT c.id, c.city, c.country
FROM customers c
WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.balance BETWEEN 29000 AND 42000);
