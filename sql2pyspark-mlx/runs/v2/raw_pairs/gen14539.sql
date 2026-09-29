SELECT
    e.id,
    e.manager_id,
    e.level,
    e.dept_id,
    d.location,
    d.budget,
    d.dept_name
FROM employees e
LEFT OUTER JOIN departments d ON d.id = e.dept_id
ORDER BY e.level ASC, e.dept_id ASC, id ASC
LIMIT 10
