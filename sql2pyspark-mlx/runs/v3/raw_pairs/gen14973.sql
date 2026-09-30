SELECT * FROM (SELECT level, AVG(salary) AS avg_salary, MAX(salary) - MIN(salary) AS salary_range FROM employees WHERE hire_date < '2024-01-01' GROUP BY level) AS t WHERE salary_range < 65;
