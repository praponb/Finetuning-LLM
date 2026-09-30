SELECT * FROM (SELECT job_title, level, salary FROM employees)
PIVOT (AVG(salary) FOR level IN ('L1', 'L3', 'L5'))
ORDER BY job_title;
