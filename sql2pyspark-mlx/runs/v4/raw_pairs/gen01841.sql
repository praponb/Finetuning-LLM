SELECT
    e.job_title,
    e.is_remote,
    d.dept_name,
    SUBSTRING(d.location, 1, 4) AS location_prefix
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id
WHERE e.dept_id IN (24, 26, 31, 39)
