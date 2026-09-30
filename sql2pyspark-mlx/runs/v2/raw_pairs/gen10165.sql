SELECT city, SUM(credit_limit) AS total_credit_limit, SUM(SUM(credit_limit)) OVER (ORDER BY city) AS cumulative_credit_limit
FROM customers
GROUP BY city
ORDER BY 2;
