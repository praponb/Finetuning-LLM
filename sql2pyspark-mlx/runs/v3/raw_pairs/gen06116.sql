SELECT
    d.location,
    d.dept_name,
    e.job_title,
    e.level
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
