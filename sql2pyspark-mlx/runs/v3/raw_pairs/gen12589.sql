SELECT * FROM (SELECT level, job_title, salary FROM employees WHERE job_title = 'Analyst') PIVOT (COUNT(salary) FOR job_title IN ('Director', 'Intern', 'Analyst')) ORDER BY level
