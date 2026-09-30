SELECT id, manager_id, hire_date, salary, SUM(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id) running_salary FROM employees WHERE NOT is_remote ORDER BY id;
