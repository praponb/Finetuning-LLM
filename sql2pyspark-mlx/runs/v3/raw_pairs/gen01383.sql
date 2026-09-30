SELECT id, email, TRIM(job_title) AS clean_job_title FROM employees WHERE job_title = 'Analyst' ORDER BY id DESC;
