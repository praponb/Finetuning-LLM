SELECT location, dept_name FROM departments WHERE dept_name IN ('Engineering', 'Finance') UNION SELECT location, dept_name FROM departments WHERE location IS NULL
