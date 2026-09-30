SELECT is_remote, CASE WHEN salary >= 118000 THEN 'high' WHEN salary >= 53000 THEN 'mid' ELSE 'low' END AS salary_bucket, COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END) AS engineer_count
FROM employees
GROUP BY is_remote, salary_bucket
HAVING COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END) >= 5
