SELECT email, 'customer' AS source FROM customers WHERE age = 36 UNION ALL SELECT email, 'employee' AS source FROM employees WHERE dept_id = 6;
