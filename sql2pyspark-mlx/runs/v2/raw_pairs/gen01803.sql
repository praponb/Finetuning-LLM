SELECT * FROM (SELECT is_remote, level, salary FROM employees)
PIVOT (MAX(salary) FOR level IN ('L1', 'L3'));
