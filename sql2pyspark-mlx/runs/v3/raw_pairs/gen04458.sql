SELECT job_title
FROM employees
WHERE hire_date >= '2023-12-13'
UNION ALL
SELECT job_title
FROM employees
WHERE job_title IS NOT NULL
ORDER BY job_title;
