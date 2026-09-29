SELECT * FROM (SELECT level, job_title, salary FROM employees WHERE LENGTH(full_name) > 8) PIVOT (MAX(salary) FOR job_title IN ('Intern', 'Designer'));
