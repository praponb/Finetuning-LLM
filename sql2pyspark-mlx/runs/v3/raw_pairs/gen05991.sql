SELECT start_date, 'subscription' AS source FROM subscriptions UNION SELECT hire_date AS start_date, 'employee' AS source FROM employees
