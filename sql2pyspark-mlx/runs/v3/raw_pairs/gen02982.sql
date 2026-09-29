SELECT YEAR(hire_date) AS hire_year, MIN(salary) AS min_salary FROM employees WHERE (NOT is_remote AND is_remote) OR dept_id IN (36, 37) GROUP BY YEAR(hire_date) HAVING MIN(salary) > 75000
