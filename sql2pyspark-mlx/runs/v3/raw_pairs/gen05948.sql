SELECT is_remote, MAX(salary) AS max_salary, COUNT(*) AS row_count FROM employees WHERE hire_date <= '2023-04-21' OR dept_id IN (6, 11, 13, 38) GROUP BY is_remote
