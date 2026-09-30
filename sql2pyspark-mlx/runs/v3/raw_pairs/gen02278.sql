SELECT a.id, a.customer_id
FROM accounts a
WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE city = 'London');
