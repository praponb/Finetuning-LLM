SELECT id, manager_id, salary, email FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS row_num FROM employees) AS r WHERE row_num = 1;
