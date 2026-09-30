SELECT
    e.id,
    e.is_remote,
    e.dept_id,
    d.budget,
    d.dept_name,
    CONCAT(d.dept_name, ' - ', d.location) AS dept_name_location
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
ORDER BY dept_name, id
LIMIT 20
