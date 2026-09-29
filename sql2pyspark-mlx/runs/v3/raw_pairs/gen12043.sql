SELECT hire_date, 'employee' source FROM employees UNION ALL SELECT start_date, 'subscription' source FROM subscriptions WHERE plan NOT IN ('basic', 'standard');
