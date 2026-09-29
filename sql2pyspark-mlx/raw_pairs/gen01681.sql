SELECT id, full_name, UPPER(TRIM(full_name)) AS full_name_normalized FROM employees WHERE manager_id IN (4, 14, 19, 40) AND dept_id IN (12, 31);
