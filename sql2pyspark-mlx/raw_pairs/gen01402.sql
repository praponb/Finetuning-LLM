SELECT id, level, manager_id, hire_date, salary, LEAD(salary, 1) OVER (PARTITION BY level, manager_id ORDER BY hire_date, id) AS next_salary FROM employees ORDER BY manager_id;
