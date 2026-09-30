SELECT id, email, job_title
FROM employees
WHERE email LIKE '%@outlook.com'
UNION ALL
SELECT id, email, job_title
FROM employees
WHERE level IS NOT NULL
ORDER BY id;
