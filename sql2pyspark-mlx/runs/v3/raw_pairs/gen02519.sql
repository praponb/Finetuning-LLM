SELECT hire_date event_date, 'employee' source
FROM employees
WHERE dept_id IN (14, 20, 26)
UNION ALL
SELECT end_date event_date, 'subscription' source
FROM subscriptions
WHERE plan = 'basic'
