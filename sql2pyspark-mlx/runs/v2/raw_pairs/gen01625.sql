SELECT level, is_remote, MIN(hire_date) AS min_hire_date FROM employees WHERE dept_id = 17 GROUP BY level, is_remote ORDER BY level
