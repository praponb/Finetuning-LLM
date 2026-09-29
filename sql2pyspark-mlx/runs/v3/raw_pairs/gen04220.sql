SELECT id, job_title, hire_date, LOWER(job_title) AS job_title_lower FROM employees WHERE email LIKE '%@outlook.com';
