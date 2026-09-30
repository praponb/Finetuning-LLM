SELECT id, is_remote, job_title
FROM employees
WHERE level IN ('L5', 'L2', 'L4')
ORDER BY id DESC
