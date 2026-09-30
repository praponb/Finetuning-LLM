SELECT
    id,
    budget,
    location,
    CONCAT_WS('-', location, dept_name) location_dept_name_key
FROM departments
ORDER BY id DESC
