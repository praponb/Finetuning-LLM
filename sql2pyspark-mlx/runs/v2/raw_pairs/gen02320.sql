SELECT is_remote, COUNT(CASE WHEN job_title = 'Analyst' THEN 1 END) AS analyst_count FROM employees GROUP BY is_remote;
