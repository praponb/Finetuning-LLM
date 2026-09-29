SELECT *, CONCAT_WS(',', dept_name, location) dept_name_location_key, UPPER(location) location_upper
FROM departments
WHERE dept_name NOT IN ('Legal', 'Engineering', 'Finance')
