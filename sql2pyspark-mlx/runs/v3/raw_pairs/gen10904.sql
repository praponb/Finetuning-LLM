SELECT id, manager_id, job_title, SUBSTRING(email, 1, 2) AS email_prefix FROM employees
