SELECT
    e.id,
    e.email,
    e.is_remote,
    e.job_title
FROM employees e
WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE location IN ('Chiang Mai', 'Bangkok'));
