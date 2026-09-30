SELECT location, COALESCE(dept_name, 'Unknown') AS dept_name_filled, TRIM(dept_name) AS clean_dept_name
FROM departments;
