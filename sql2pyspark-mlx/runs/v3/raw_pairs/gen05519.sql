SELECT * FROM (SELECT job_title, level, salary FROM employees)
PIVOT (MAX(salary) FOR level IN ('L4', 'L3', 'L1'))
ORDER BY job_title;
