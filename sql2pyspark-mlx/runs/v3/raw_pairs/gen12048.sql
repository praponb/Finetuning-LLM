SELECT * FROM (SELECT is_remote, level, salary FROM employees)
PIVOT (COUNT(salary) FOR level IN ('L5', 'L4', 'L1'))
