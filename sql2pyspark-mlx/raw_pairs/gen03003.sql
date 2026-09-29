SELECT
    d.dept_name,
    d.location,
    e.dept_id,
    e.hire_date,
    TRIM(d.location) AS clean_location
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
