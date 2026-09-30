SELECT
    e.id,
    e.job_title,
    e.dept_id,
    d.location,
    d.budget
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id;
