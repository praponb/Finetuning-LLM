SELECT id, job_title, manager_id, salary, UPPER(full_name) full_name_upper, TRIM(level) clean_level FROM employees WHERE manager_id IS NOT NULL ORDER BY full_name_upper
