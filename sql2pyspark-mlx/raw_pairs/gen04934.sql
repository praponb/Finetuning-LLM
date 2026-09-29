SELECT
    dept_name,
    budget,
    SUBSTRING(location, 1, 3) AS location_prefix,
    CONCAT_WS('-', dept_name, location) AS dept_name_location_key
FROM departments
WHERE location <> 'Berlin' OR location IN ('Chiang Mai', 'Bangkok', 'London')
ORDER BY dept_name
