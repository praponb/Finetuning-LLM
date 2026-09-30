SELECT
    e.id,
    e.dept_id,
    e.salary,
    e.job_title
FROM employees e
WHERE e.email IS NOT NULL AND e.dept_id IN (SELECT id FROM departments WHERE location = 'Tokyo')
ORDER BY id DESC
