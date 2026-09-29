SELECT id, manager_id, hire_date, NTILE(10) OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) AS quartile, COUNT(*) OVER (PARTITION BY manager_id) AS manager_id_count FROM employees
