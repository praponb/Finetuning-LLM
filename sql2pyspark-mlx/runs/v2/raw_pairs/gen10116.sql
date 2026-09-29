SELECT id, hire_date, salary, LEAD(salary, 1) OVER (ORDER BY hire_date, id) AS next_salary FROM employees ORDER BY next_salary DESC
