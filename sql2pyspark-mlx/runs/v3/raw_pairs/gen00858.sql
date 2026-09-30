SELECT end_date, 'subscription' source FROM subscriptions UNION ALL SELECT hire_date end_date, 'employee' source FROM employees WHERE NOT is_remote
