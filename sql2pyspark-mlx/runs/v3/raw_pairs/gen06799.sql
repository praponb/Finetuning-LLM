SELECT * FROM (SELECT level, job_title, salary FROM employees) PIVOT (MAX(salary) FOR job_title IN ('Director', 'Intern'));
