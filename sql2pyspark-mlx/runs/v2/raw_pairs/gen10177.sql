SELECT * FROM (SELECT id, manager_id, hire_date, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) AS row_num FROM employees) AS t WHERE row_num <= 3;
