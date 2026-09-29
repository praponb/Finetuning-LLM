SELECT id, manager_id, hire_date, salary, salary - LAG(salary, 1) OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS salary_change FROM employees WHERE job_title IN ('Designer', 'Director')
