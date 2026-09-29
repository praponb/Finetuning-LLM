SELECT is_remote, MIN(hire_date) AS first_hire_date FROM employees GROUP BY is_remote ORDER BY first_hire_date DESC;
