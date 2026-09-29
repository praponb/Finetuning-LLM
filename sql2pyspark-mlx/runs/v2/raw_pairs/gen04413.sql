SELECT * FROM (SELECT job_title, level, salary FROM employees)
PIVOT (SUM(salary) FOR level IN ('L3', 'L4', 'L1'))
