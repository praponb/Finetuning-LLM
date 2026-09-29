SELECT manager_id, email FROM employees WHERE job_title = 'Manager' UNION SELECT manager_id, email FROM employees WHERE salary BETWEEN 70000 AND 100000 ORDER BY email DESC
