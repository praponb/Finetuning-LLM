SELECT id, salary, job_title, level, SUBSTRING(email, 1, 3) AS email_prefix FROM employees WHERE is_remote AND (salary < 89000 OR salary >= 110000)
