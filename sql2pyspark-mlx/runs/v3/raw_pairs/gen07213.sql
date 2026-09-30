SELECT * FROM employees WHERE MONTH(hire_date) IN (5, 7, 12) ORDER BY full_name ASC, id ASC LIMIT 10;
