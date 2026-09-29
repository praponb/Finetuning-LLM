SELECT dept_id, is_remote, email, CONCAT_WS('/', job_title, level) job_title_level_key FROM employees
