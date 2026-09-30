SELECT hire_date, 'employee' AS record_type
FROM employees
WHERE hire_date IS NOT NULL
UNION
SELECT end_date AS hire_date, 'subscription' AS record_type
FROM subscriptions
WHERE end_date IS NOT NULL;
