SELECT MIN(credit_limit) AS min_credit_limit
FROM customers
WHERE city IN ('Bangkok', 'Singapore', 'London')
ORDER BY min_credit_limit DESC
