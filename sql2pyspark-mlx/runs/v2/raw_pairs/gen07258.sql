SELECT
    budget,
    location,
    dept_name,
    CONCAT(location, ' - ', dept_name) AS location_dept_name
FROM departments
WHERE dept_name NOT IN ('Sales', 'Marketing')
