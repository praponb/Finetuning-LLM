SELECT email, is_remote, job_title, LENGTH(level) AS level_length FROM employees WHERE job_title IN ('Intern', 'Director');
