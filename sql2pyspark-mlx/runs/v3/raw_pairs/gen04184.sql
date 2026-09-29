SELECT id, dept_name FROM departments WHERE budget < 3168000 UNION ALL SELECT id, dept_name FROM departments WHERE dept_name IN ('Legal', 'HR', 'Finance') ORDER BY dept_name
