SELECT level, SUM(salary) AS total_salary, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(salary) DESC) AS salary_rank FROM employees WHERE manager_id IN (7, 21, 34) GROUP BY level
