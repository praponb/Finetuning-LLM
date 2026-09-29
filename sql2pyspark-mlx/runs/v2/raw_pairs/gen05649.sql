SELECT
    id,
    location,
    dept_name,
    budget,
    LOWER(location) AS location_lower
FROM departments
WHERE location IN ('Berlin', 'Singapore');
