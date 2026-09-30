SELECT YEAR(hire_date) AS hire_year, MAX(salary) AS max_salary FROM employees WHERE LENGTH(full_name) > 12 AND is_remote GROUP BY 1;
