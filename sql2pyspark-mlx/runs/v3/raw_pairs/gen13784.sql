SELECT * FROM (SELECT job_title, level, salary FROM employees)
PIVOT (COUNT(salary) FOR level IN ('L3', 'L4', 'L2'))
ORDER BY job_title;
