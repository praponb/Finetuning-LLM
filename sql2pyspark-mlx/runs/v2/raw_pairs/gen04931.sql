SELECT id, level, LENGTH(job_title) AS job_title_length FROM employees WHERE UPPER(full_name) LIKE '%RAN%' AND job_title NOT IN ('Intern', 'Engineer', 'Director');
