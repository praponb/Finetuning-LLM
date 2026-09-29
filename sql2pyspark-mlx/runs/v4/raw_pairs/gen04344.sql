SELECT level, MIN(salary) AS min_salary, MIN(hire_date) AS first_hire_date FROM employees GROUP BY level HAVING min_salary >= 128000 ORDER BY 3, min_salary DESC
