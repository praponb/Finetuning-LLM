SELECT id, level FROM employees WHERE manager_id IN (21, 34, 35, 37) UNION ALL SELECT id, level FROM employees WHERE UPPER(TRIM(full_name)) = 'HENRY FORD'
