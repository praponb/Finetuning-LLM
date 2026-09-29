SELECT id, dept_id, level, salary, MAX(salary) OVER (PARTITION BY dept_id, level) AS dept_id_max_salary FROM employees ORDER BY level
