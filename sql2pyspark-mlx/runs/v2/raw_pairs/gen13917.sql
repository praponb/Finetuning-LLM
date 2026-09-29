SELECT id, email, hire_date, job_title, CAST(is_remote AS INT) AS is_remote_flag FROM employees WHERE full_name LIKE '%Ross%';
