SELECT
    e.id,
    e.dept_id,
    e.email,
    d.budget,
    d.dept_name,
    d.location
FROM employees e
FULL OUTER JOIN departments d ON e.dept_id = d.id
ORDER BY id, 4 DESC;
