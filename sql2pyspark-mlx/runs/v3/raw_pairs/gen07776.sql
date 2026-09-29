SELECT * FROM (SELECT is_remote, job_title, salary FROM employees)
PIVOT (AVG(salary) FOR job_title IN ('Analyst', 'Intern', 'Engineer'));
