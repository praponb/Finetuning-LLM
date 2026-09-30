SELECT country
FROM customers
WHERE country IS NOT NULL
UNION
SELECT job_title
FROM employees
WHERE job_title IS NOT NULL
