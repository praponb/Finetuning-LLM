SELECT id, manager_id, hire_date, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS hire_date_row FROM employees WHERE full_name LIKE 'Mia%'
