SELECT id, level, hire_date, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS row_num FROM employees;
