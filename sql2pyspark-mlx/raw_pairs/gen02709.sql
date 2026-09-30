SELECT
    e.id,
    e.is_remote,
    e.job_title,
    d.budget
FROM employees e
LEFT OUTER JOIN departments d ON d.id = e.dept_id
WHERE d.budget >= 3600000
