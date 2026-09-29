SELECT id, salary, manager_id, email, LENGTH(level) AS level_length FROM employees WHERE dept_id IN (8, 28);
