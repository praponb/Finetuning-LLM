SELECT
    id,
    dept_name,
    location,
    budget,
    CONCAT(dept_name, '/', location) AS dept_name_location
FROM departments
