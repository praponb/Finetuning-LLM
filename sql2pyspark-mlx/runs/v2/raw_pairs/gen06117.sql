SELECT
    d.budget,
    d.dept_name,
    d.location,
    e.manager_id,
    e.level
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.level = 'L2'
ORDER BY 1;
