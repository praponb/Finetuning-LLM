SELECT id, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) AS salary_row FROM employees WHERE NOT is_remote;
