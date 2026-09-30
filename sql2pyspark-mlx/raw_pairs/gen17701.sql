SELECT dept_id, level, SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END) manager_count FROM employees WHERE UPPER(email) LIKE '%ENR%' GROUP BY dept_id, level;
