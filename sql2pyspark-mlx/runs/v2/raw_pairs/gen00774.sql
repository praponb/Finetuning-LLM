SELECT * FROM (SELECT manager_id, job_title, salary FROM employees WHERE manager_id IN (1, 23, 31, 35))
PIVOT (MAX(salary) FOR job_title IN ('Designer', 'Intern', 'Director'))
