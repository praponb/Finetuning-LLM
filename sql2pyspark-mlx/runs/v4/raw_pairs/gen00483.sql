SELECT
    e.hire_date,
    e.full_name,
    e.is_remote,
    d.dept_name,
    UPPER(d.location) location_upper
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE e.email IS NOT NULL
ORDER BY full_name, location_upper DESC;
