SELECT
    e.id,
    e.job_title,
    d.budget,
    d.location
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id
ORDER BY location, id
LIMIT 50
