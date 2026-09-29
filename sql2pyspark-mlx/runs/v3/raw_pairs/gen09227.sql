SELECT a.customer_id, c.email, c.age
FROM accounts a
JOIN customers c ON a.customer_id = c.id
