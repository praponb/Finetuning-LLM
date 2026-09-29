SELECT email, full_name, LOWER(job_title) AS job_title_lower FROM employees WHERE email LIKE '%@outlook.com'
