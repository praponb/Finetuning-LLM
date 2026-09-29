SELECT * FROM (SELECT level, job_title, salary FROM employees)
PIVOT (SUM(salary) FOR job_title IN ('Analyst', 'Engineer', 'Director', 'Designer'))
ORDER BY level;
