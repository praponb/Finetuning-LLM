SELECT id, email, credit_limit
FROM customers
WHERE name LIKE 'No%' OR credit_limit > 7100;
