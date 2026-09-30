SELECT level, COUNT(CASE WHEN job_title = 'Director' THEN 1 END) AS director_count FROM employees WHERE dept_id IN (17, 18, 31) GROUP BY level;
