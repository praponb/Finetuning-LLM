SELECT job_title, is_remote, CAST(is_remote AS INT) is_remote_flag, CASE WHEN level = 'L5' THEN 1 ELSE 0 END is_l5 FROM employees WHERE salary <= 98000 AND job_title = 'Director';
