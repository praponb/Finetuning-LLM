SELECT
    d.location,
    d.budget,
    d.dept_name,
    e.job_title,
    e.dept_id
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.level = 'L2'
