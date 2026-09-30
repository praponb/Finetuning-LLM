SELECT
    id,
    location,
    budget,
    dept_name,
    INITCAP(TRIM(location)) location_title
FROM departments
WHERE location IN ('Berlin', 'Singapore', 'London') AND location IS NULL AND location = 'Singapore';
