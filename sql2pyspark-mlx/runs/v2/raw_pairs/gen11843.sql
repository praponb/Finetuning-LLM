SELECT YEAR(hire_date) AS hire_year, COUNT(DISTINCT salary) AS unique_depts, COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count FROM employees WHERE dept_id = 22 GROUP BY hire_year;
