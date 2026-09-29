SELECT email FROM customers EXCEPT SELECT email FROM employees WHERE job_title = 'Manager' ORDER BY email
