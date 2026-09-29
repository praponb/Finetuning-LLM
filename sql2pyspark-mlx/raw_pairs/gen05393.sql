SELECT level, 'employee' origin
FROM employees
UNION
SELECT status, 'subscription' origin
FROM subscriptions
