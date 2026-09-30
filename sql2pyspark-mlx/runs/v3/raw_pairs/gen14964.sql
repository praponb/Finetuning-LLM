SELECT city, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit, COUNT(*) AS customer_count
FROM customers
WHERE age > 38
GROUP BY city
HAVING COUNT(*) >= 1
ORDER BY customer_count DESC;
