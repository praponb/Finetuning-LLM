SELECT dept_id, email, CONCAT(full_name, ' - ', email) AS full_name_email
FROM employees
WHERE job_title = 'Manager';
