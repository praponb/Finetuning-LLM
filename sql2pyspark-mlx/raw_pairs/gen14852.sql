SELECT id, salary, SUBSTRING(level, 1, 2) AS level_prefix, CONCAT(job_title, ' - ', level) AS job_title_level FROM employees WHERE dept_id = 8 OR job_title = 'Manager'
