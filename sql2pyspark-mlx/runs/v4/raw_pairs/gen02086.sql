SELECT hire_date
FROM employees
UNION ALL
SELECT start_date
FROM subscriptions
ORDER BY hire_date DESC
