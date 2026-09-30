SELECT job_title, manager_id, COALESCE(level, 'Unknown') level_filled, TRIM(full_name) clean_full_name FROM employees WHERE level = 'L1';
