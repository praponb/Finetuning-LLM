SELECT id, level, hire_date FROM (SELECT id, level, hire_date, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS pos FROM employees WHERE full_name LIKE '%Oliv%') AS r WHERE pos <= 1;
