SELECT email, 'customer' AS source FROM customers UNION ALL SELECT email, 'employee' AS source FROM employees WHERE manager_id = 25
