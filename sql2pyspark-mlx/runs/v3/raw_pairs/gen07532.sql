SELECT id, level, hire_date, salary, LEAD(salary, 2) OVER (PARTITION BY level ORDER BY hire_date, id) AS next_salary FROM employees WHERE dept_id IN (20, 38) ORDER BY level ASC;
