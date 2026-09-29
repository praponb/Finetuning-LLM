SELECT *, LOWER(level) AS level_lower
FROM employees
WHERE manager_id IS NOT NULL
ORDER BY id DESC, job_title
LIMIT 3
