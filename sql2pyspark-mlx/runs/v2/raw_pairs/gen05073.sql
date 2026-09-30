SELECT id, manager_id, hire_date, salary, LAG(salary, 1, 0) OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS prev_salary FROM employees ORDER BY 4 DESC;
