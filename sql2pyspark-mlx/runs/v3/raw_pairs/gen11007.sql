SELECT id, manager_id, salary, ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 3) AS salary_share FROM employees WHERE dept_id IN (12, 19, 24, 27) ORDER BY salary
