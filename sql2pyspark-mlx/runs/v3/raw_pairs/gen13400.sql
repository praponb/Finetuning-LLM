SELECT id, level, hire_date, NTILE(3) OVER (PARTITION BY level ORDER BY hire_date, id) AS bucket FROM employees;
