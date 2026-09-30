SELECT id, level, hire_date, salary, LAG(salary, 1) OVER (PARTITION BY level ORDER BY hire_date, id) AS prev_salary FROM employees ORDER BY hire_date;
