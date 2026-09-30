SELECT id, dept_id, salary, ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 3) AS salary_share FROM employees WHERE manager_id IS NOT NULL;
