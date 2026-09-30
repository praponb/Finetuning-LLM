SELECT dept_name, location, TRIM(dept_name) AS clean_dept_name, LENGTH(location) AS location_length FROM departments WHERE dept_name IN ('Engineering', 'HR') ORDER BY clean_dept_name;
