SELECT id, dept_id FROM employees WHERE level IN ('L2', 'L5') UNION SELECT id, dept_id FROM employees WHERE salary <= 134000 ORDER BY dept_id
