SELECT * FROM (SELECT level, job_title, salary FROM employees) PIVOT (SUM(salary) FOR job_title IN ('Engineer', 'Designer', 'Analyst'));
