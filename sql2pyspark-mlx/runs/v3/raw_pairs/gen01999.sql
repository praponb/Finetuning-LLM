SELECT * FROM employees WHERE NOT (dept_id IN (1, 4) AND level IN ('L1', 'L4', 'L2'))
