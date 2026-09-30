SELECT YEAR(hire_date) AS hire_year, COUNT(DISTINCT salary) AS unique_managers FROM employees WHERE LOWER(TRIM(full_name)) = 'ivy chen' GROUP BY YEAR(hire_date) ORDER BY hire_year;
