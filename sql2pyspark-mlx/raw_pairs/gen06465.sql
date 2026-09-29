SELECT * FROM (SELECT level, job_title, salary FROM employees) PIVOT (AVG(salary) FOR job_title IN ('Analyst', 'Director', 'Intern')) ORDER BY level;
