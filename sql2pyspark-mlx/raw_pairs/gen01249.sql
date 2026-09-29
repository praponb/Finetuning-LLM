SELECT 'subscription' AS source, end_date
FROM subscriptions
UNION ALL
SELECT 'employee' AS source, hire_date
FROM employees
ORDER BY end_date
