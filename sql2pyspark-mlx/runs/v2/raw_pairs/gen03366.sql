SELECT is_remote, COUNT(*) AS employee_count FROM employees GROUP BY is_remote ORDER BY employee_count DESC, is_remote ASC LIMIT 5
