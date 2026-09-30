SELECT id, level, hire_date, salary, LEAD(salary, 1) OVER (PARTITION BY level ORDER BY hire_date, id) AS next_salary FROM employees WHERE manager_id IN (3, 35, 37, 40)
