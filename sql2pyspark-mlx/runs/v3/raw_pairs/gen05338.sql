SELECT id, level, hire_date, salary, salary - LAG(salary, 1) OVER (PARTITION BY level ORDER BY hire_date, id) AS salary_change FROM employees WHERE manager_id = 18
