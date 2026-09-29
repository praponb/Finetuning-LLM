SELECT * FROM (SELECT level, job_title, salary FROM employees)
PIVOT (SUM(salary) FOR job_title IN ('Intern', 'Manager'))
ORDER BY level;
