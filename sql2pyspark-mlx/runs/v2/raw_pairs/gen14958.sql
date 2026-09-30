SELECT id, level, salary, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) rn FROM employees WHERE email LIKE '%@company.co'
