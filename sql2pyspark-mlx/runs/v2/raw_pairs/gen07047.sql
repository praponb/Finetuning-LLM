SELECT * FROM (SELECT is_remote, level, salary FROM employees)
PIVOT (SUM(salary) FOR level IN ('L5', 'L4', 'L1'))
ORDER BY is_remote;
