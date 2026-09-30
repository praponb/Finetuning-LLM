SELECT country, ROUND(AVG(credit_limit), 2) AS avg_credit_limit, MIN(credit_limit) AS min_credit_limit
FROM customers
WHERE age <> 55
GROUP BY country
ORDER BY country
