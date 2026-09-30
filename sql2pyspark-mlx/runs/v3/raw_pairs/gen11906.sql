SELECT id, full_name, dept_id, CONCAT_WS('/', job_title, full_name) AS job_title_full_name_key FROM employees WHERE dept_id IN (25, 26, 34, 40)
