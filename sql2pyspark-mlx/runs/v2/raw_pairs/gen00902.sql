SELECT * FROM (SELECT is_remote, level, salary FROM employees WHERE is_remote) PIVOT (AVG(salary) FOR level IN ('L1', 'L5')) ORDER BY is_remote;
