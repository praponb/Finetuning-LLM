SELECT * FROM (SELECT manager_id, dept_id, job_title, salary FROM employees) src PIVOT (SUM(salary) AS amount, COUNT(1) AS cnt FOR job_title IN ('Engineer', 'Intern'));
