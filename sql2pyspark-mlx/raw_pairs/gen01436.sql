SELECT * FROM (SELECT is_remote, job_title, salary FROM employees) PIVOT (COUNT(salary) FOR job_title IN ('Manager', 'Director')) ORDER BY is_remote;
