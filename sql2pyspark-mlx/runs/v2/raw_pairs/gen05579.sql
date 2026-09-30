SELECT email FROM customers WHERE name LIKE 'Ol%' UNION SELECT email FROM employees WHERE dept_id = 31 ORDER BY email
