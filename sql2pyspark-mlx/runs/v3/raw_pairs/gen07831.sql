SELECT
    id,
    budget,
    dept_name,
    TRIM(location) AS clean_location
FROM departments
WHERE location IN ('Chiang Mai', 'Tokyo', 'London')
