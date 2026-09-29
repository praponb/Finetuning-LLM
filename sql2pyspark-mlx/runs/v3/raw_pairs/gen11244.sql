SELECT location, dept_name FROM departments WHERE dept_name = 'Sales' UNION ALL SELECT location, dept_name FROM departments WHERE dept_name IS NOT NULL ORDER BY location
