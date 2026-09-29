SELECT
    e.id,
    e.email,
    e.job_title,
    d.dept_name,
    d.budget
FROM employees e
LEFT JOIN departments d ON d.id = e.dept_id
WHERE d.budget > 1101000 AND e.dept_id = 18
ORDER BY email, d.budget DESC
