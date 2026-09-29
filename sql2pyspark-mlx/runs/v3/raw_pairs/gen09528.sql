SELECT id, dept_id, salary, is_remote, CONCAT_WS('-', job_title, email) AS job_title_email_key FROM employees
