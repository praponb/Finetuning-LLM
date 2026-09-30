SELECT a.id, a.status
FROM accounts a
WHERE a.customer_id IN (SELECT id FROM customers WHERE city IS NULL)
