SELECT * FROM (SELECT level, job_title, salary FROM employees WHERE job_title = 'Engineer') PIVOT (AVG(salary) FOR job_title IN ('Engineer', 'Analyst', 'Designer')) ORDER BY level;
