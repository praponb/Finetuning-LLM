SELECT dept_id, manager_id FROM employees EXCEPT SELECT dept_id, manager_id FROM employees WHERE LOWER(TRIM(email)) = 'ivy@gmail.com';
