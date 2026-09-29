SELECT level FROM employees WHERE UPPER(TRIM(email)) = 'IVY@GMAIL.COM' UNION ALL SELECT level FROM employees WHERE level IS NOT NULL ORDER BY level
