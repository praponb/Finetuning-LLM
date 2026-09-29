SELECT
    e.id,
    e.job_title,
    e.full_name,
    e.level
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE budget > 2054000)
