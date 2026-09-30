SELECT
    id,
    dept_name,
    budget,
    location,
    COALESCE(dept_name, 'unassigned') AS dept_name_filled,
    CONCAT_WS('-', location, dept_name) AS location_dept_name_key
FROM departments
WHERE dept_name = 'Finance' AND budget >= 2820000
