SELECT id, dept_id, salary, MAX(salary) OVER (PARTITION BY dept_id) AS dept_id_max_salary FROM employees WHERE dept_id IN (8, 33) ORDER BY dept_id;
