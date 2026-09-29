SELECT city, SUM(credit_limit) AS total_credit_limit, SUM(SUM(credit_limit)) OVER (ORDER BY city) AS cumulative_credit_limit
FROM customers
WHERE credit_limit BETWEEN 5100 AND 13700
GROUP BY city
ORDER BY cumulative_credit_limit DESC;
