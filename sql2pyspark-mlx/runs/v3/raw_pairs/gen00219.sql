SELECT country, MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range
FROM customers
GROUP BY country
HAVING MAX(credit_limit) - MIN(credit_limit) >= 63
ORDER BY country
LIMIT 20
