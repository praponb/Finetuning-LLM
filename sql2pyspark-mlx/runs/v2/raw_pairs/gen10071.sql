SELECT * FROM employees WHERE NOT (level IN ('L4', 'L1') OR level = 'L2') ORDER BY level
