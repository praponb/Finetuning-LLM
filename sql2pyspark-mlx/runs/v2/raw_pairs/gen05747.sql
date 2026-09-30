SELECT id, level, email, LOWER(job_title) AS job_title_lower, SPLIT(LOWER(email), '@')[0] AS email_user FROM employees;
