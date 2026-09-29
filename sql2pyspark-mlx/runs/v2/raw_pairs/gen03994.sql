SELECT id, email, is_remote, dept_id, SUBSTRING(level, 1, 3) AS level_prefix, LENGTH(job_title) AS job_title_length FROM employees WHERE UPPER(full_name) LIKE '%AVI%';
