SELECT email FROM customers WHERE name LIKE '%Wong%' INTERSECT SELECT email FROM employees WHERE job_title = 'Engineer' ORDER BY email DESC;
