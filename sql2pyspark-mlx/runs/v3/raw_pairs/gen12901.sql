SELECT * FROM (SELECT level, job_title, salary FROM employees WHERE LENGTH(email) > 13) PIVOT (AVG(salary) FOR job_title IN ('Intern', 'Engineer'))
