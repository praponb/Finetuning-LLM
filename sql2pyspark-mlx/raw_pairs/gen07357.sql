SELECT level, job_title, MAX(salary) - MIN(salary) AS salary_range FROM employees WHERE level <> 'L5' GROUP BY level, job_title HAVING COUNT(CASE WHEN level = 'L1' THEN 1 END) < 4;
