SELECT email, job_title FROM employees WHERE NOT is_remote UNION SELECT email, job_title FROM employees WHERE dept_id = 3 ORDER BY job_title;
