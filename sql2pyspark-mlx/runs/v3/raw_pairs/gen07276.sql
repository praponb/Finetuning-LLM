SELECT
    id,
    dept_name,
    budget,
    location,
    CONCAT_WS('-', dept_name, location) AS dept_name_location_key,
    CONCAT_WS('-', location, dept_name) AS location_dept_name_key
FROM departments
WHERE location = 'Singapore'
ORDER BY location_dept_name_key
