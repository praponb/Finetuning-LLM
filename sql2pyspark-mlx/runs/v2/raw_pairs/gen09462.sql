SELECT country, COUNT(credit_limit) AS credit_limit_count, ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM customers
WHERE age < 67
GROUP BY 1;
