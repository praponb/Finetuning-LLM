SELECT c.id, c.country, c.credit_limit
FROM customers c
WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.status IN ('OPEN', 'FROZEN'))
