SELECT hire_date
FROM employees
WHERE salary BETWEEN 75000 AND 109000
INTERSECT
SELECT hire_date
FROM employees
WHERE is_remote
ORDER BY hire_date
