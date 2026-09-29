SELECT id, credit_limit
FROM customers
WHERE credit_limit < (SELECT MIN(credit_limit) FROM customers)
ORDER BY id DESC
LIMIT 1
