SELECT id, manager_id, salary, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) row_num FROM employees WHERE salary <= 147000
