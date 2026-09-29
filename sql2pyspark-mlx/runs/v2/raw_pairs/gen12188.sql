SELECT * FROM (SELECT job_title, level, salary FROM employees) PIVOT (SUM(salary) FOR level IN ('L3', 'L1', 'L5')) ORDER BY job_title;
