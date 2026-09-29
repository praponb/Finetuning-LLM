SELECT
    e.id,
    e.full_name,
    e.is_remote,
    d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.id
WHERE e.job_title NOT IN ('Intern', 'Manager')
ORDER BY id DESC;
