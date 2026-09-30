SELECT level, is_remote, MIN(hire_date) AS first_hire_date
FROM employees
GROUP BY level, is_remote
ORDER BY level;
