SELECT level, is_remote, SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END) AS l2_count FROM employees WHERE job_title = 'Manager' AND manager_id = 6 GROUP BY level, is_remote
