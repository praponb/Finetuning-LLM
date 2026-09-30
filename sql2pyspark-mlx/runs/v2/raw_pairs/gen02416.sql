SELECT id, email, hire_date, job_title, SUBSTRING(job_title, 1, 3) AS job_title_prefix FROM employees WHERE salary BETWEEN 82000 AND 94000 ORDER BY id
