SELECT
    budget,
    dept_name,
    location,
    UPPER(location) AS location_upper,
    UPPER(location) AS location_upper_2
FROM departments
WHERE budget >= 2728000 AND dept_name NOT IN ('Finance', 'Sales')
