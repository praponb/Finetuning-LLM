SELECT * FROM (SELECT job_title, level, salary FROM employees) PIVOT (COUNT(salary) FOR level IN ('L3', 'L1', 'L2', 'L5')) ORDER BY job_title;
