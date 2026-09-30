SELECT id, dept_id, salary, MIN(salary) OVER (PARTITION BY dept_id) AS dept_id_min_salary FROM employees WHERE manager_id IN (8, 20, 39) ORDER BY id DESC;
