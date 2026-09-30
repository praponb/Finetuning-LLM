SELECT is_active, country, COUNT(credit_limit) AS credit_limit_count
FROM customers
GROUP BY 1, 2
HAVING COUNT(credit_limit) > 4;
