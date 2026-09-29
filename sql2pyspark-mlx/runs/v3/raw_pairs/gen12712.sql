SELECT
    e.id,
    e.level,
    e.is_remote,
    e.job_title
FROM employees e
WHERE e.dept_id IN (10, 15, 27, 33) AND e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'Engineering')
ORDER BY e.level ASC
