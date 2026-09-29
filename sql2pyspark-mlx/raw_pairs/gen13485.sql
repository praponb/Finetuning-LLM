SELECT id, manager_id, salary, PERCENT_RANK() OVER (PARTITION BY manager_id ORDER BY salary) AS salary_pct_rank FROM employees WHERE email LIKE '%@outlook.com';
