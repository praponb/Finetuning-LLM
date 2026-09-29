SELECT * FROM (SELECT job_title, level, salary FROM employees WHERE manager_id IN (1, 3, 4)) PIVOT (SUM(salary) FOR level IN ('L1', 'L5', 'L2', 'L4'))
