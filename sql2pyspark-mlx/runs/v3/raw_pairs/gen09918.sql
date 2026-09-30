SELECT country, COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count, MIN(credit_limit) AS min_credit_limit
FROM customers
GROUP BY country
HAVING COUNT(DISTINCT age) < 2
