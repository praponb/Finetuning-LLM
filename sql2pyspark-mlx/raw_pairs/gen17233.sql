SELECT id, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS row_num FROM employees WHERE manager_id IN (1, 13, 21);
