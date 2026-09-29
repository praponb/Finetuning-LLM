SELECT id, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) AS row_num FROM employees WHERE is_remote = TRUE
