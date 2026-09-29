SELECT
    d.dept_name,
    d.location,
    e.dept_id,
    e.email,
    e.manager_id,
    LENGTH(d.dept_name) AS dept_name_length
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
WHERE d.dept_name = 'Marketing' OR e.manager_id IS NOT NULL;
