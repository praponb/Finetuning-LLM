SELECT
    e.id,
    e.dept_id,
    e.full_name,
    d.dept_name,
    LOWER(d.location) location_lower
FROM employees e
INNER JOIN departments d ON d.id = e.dept_id
WHERE d.dept_name = 'Legal' OR e.full_name IS NOT NULL
ORDER BY id DESC, location_lower ASC
LIMIT 5
