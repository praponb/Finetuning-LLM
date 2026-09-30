SELECT
    id,
    location,
    SUBSTRING(dept_name, 1, 3) AS dept_name_prefix,
    LOWER(location) AS location_lower
FROM departments
WHERE location IN ('Chiang Mai', 'London', 'Tokyo') OR dept_name IN ('Sales', 'HR');
