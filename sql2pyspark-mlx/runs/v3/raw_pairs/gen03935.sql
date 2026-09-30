SELECT a.id, a.status
FROM accounts a
WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE age IS NOT NULL);
