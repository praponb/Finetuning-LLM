SELECT id, level, salary, AVG(salary) OVER (PARTITION BY level) level_avg_salary FROM employees WHERE full_name LIKE '%Pim%' ORDER BY salary DESC
