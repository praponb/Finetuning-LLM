SELECT
    d.budget,
    d.dept_name,
    e.is_remote,
    e.full_name,
    e.email
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id
WHERE e.dept_id IN (2, 3, 10) AND LOWER(e.email) LIKE '%aro%'
