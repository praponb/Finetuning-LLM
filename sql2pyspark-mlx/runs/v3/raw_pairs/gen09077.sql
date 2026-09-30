SELECT is_active, COUNT(*) AS customer_count, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
FROM customers
WHERE age = 52
GROUP BY is_active;
