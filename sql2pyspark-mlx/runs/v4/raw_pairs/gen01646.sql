SELECT * FROM (SELECT manager_id, dept_id, job_title, salary FROM employees) src PIVOT (AVG(salary) AS value_sum FOR job_title IN ('Engineer', 'Intern', 'Analyst'))
