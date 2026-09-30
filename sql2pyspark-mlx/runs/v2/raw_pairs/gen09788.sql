SELECT * FROM (SELECT job_title, level, salary FROM employees WHERE dept_id IN (10, 19, 34)) PIVOT (AVG(salary) FOR level IN ('L4', 'L1', 'L2'))
