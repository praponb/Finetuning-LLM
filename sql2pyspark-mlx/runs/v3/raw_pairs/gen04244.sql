SELECT is_active, AVG(credit_limit) AS avg_credit_limit
FROM customers
GROUP BY is_active
HAVING AVG(credit_limit) > 4500
ORDER BY avg_credit_limit DESC
