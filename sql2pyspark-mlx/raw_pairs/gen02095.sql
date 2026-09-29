SELECT is_remote, ROUND((100 * SUM(CASE WHEN job_title = 'Director' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_director FROM employees WHERE salary < 133000 OR manager_id IS NULL GROUP BY is_remote;
