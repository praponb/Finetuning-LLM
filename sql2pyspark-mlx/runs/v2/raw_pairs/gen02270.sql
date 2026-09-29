SELECT level FROM employees WHERE is_remote = TRUE UNION SELECT level FROM employees WHERE dept_id IN (13, 18, 23, 37);
