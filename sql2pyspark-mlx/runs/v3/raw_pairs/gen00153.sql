SELECT id, location, dept_name FROM departments WHERE dept_name IN ('Marketing', 'Engineering') UNION SELECT id, location, dept_name FROM departments WHERE location IS NOT NULL;
