SELECT id, level, salary, hire_date, SUM(salary) OVER (PARTITION BY level) AS level_sum_salary, SUM(salary) OVER (ORDER BY hire_date, id) AS running_salary FROM employees WHERE salary < 63000
