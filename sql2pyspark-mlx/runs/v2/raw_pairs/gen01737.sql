SELECT
    d.location,
    d.budget,
    e.job_title,
    e.is_remote,
    e.dept_id
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
ORDER BY e.job_title DESC;
