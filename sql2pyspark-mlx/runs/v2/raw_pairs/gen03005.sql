SELECT id, level, salary, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS row_num FROM employees WHERE full_name LIKE '%MILLER%'
