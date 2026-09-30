SELECT id, manager_id, hire_date, RANK() OVER (PARTITION BY manager_id ORDER BY hire_date DESC) AS rnk FROM employees;
