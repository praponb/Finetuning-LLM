SELECT
    id,
    dept_name,
    location,
    SUBSTRING(location, 1, 3) AS location_prefix,
    TRIM(location) AS clean_location
FROM departments
WHERE dept_name <> 'HR';
