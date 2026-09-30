SELECT 'employee' AS kind, hire_date AS event_date FROM employees UNION ALL SELECT 'customer' AS kind, signup_date AS event_date FROM customers WHERE age <> 40
