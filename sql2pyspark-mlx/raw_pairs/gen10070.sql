SELECT * FROM (SELECT job_title, level, salary FROM employees) src PIVOT (AVG(salary) FOR level IN ('L1', 'L5', 'L2'))
