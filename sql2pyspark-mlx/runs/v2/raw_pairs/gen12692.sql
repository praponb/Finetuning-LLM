SELECT id, dept_id, email, UPPER(TRIM(full_name)) AS full_name_normalized FROM employees WHERE email IS NULL AND salary >= 64000 ORDER BY email, id LIMIT 50;
