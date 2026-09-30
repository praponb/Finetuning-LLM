SELECT * FROM (SELECT dept_id, level, salary FROM employees) src PIVOT (MAX(salary) FOR level IN ('L4', 'L5', 'L1'));
