SELECT is_remote, COUNT(*) AS employee_count FROM employees WHERE dept_id IN (3, 7) GROUP BY is_remote ORDER BY 2 DESC;
