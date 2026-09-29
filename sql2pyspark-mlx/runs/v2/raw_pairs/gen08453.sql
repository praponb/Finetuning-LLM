SELECT id, dept_id, salary, TRIM(job_title) AS clean_job_title FROM employees WHERE email LIKE '%@outlook.com';
