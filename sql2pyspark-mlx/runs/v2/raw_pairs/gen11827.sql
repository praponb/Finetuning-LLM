SELECT location, CONCAT_WS(',', dept_name, location) AS dept_name_location_key
FROM departments
WHERE dept_name IN ('Marketing', 'HR');
