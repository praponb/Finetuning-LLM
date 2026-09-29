SELECT email
FROM employees
WHERE salary BETWEEN 97000 AND 128000
UNION ALL
SELECT email
FROM employees
WHERE job_title IN ('Director', 'Engineer', 'Intern')
