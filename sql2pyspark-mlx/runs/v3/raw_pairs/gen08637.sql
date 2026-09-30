SELECT id, manager_id, hire_date, RANK() OVER (PARTITION BY manager_id ORDER BY hire_date DESC) hire_date_rank FROM employees WHERE level IN ('L5', 'L1');
