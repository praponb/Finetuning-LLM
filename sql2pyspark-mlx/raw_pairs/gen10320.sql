SELECT 'order' AS kind, status
FROM orders
UNION
SELECT 'employee' AS kind, job_title
FROM employees
ORDER BY status;
