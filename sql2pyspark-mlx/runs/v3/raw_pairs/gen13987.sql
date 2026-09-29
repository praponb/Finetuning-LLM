SELECT dept_name, location FROM departments WHERE dept_name IN ('Engineering', 'Legal') UNION SELECT dept_name, location FROM departments WHERE location != 'Bangkok' ORDER BY location
