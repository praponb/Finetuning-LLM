SELECT id, hire_date, NTILE(2) OVER (ORDER BY hire_date DESC, id) AS quartile FROM employees WHERE level = 'L2';
