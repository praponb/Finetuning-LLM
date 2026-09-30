SELECT id, dept_name FROM departments WHERE budget >= 2598000 UNION ALL SELECT id, dept_name FROM departments WHERE dept_name IN ('Legal', 'HR')
