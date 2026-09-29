SELECT email, 'customer' AS source FROM customers UNION SELECT email, 'employee' AS source FROM employees WHERE dept_id IN (9, 21) ORDER BY source;
