SELECT email, 'customer' AS kind FROM customers UNION SELECT email, 'employee' AS kind FROM employees WHERE job_title IN ('Designer', 'Intern', 'Analyst') ORDER BY email DESC
