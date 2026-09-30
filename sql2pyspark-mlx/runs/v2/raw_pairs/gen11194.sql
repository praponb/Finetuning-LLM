SELECT email, level FROM employees WHERE manager_id IS NULL UNION SELECT email, level FROM employees WHERE dept_id = 10
