SELECT manager_id, level, is_remote, COALESCE(job_title, 'Unknown') AS job_title_filled FROM employees WHERE job_title = 'Director' ORDER BY manager_id DESC
