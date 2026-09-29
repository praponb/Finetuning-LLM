SELECT job_title AS label
FROM employees
WHERE job_title IS NOT NULL
EXCEPT
SELECT method AS label
FROM payments
WHERE method IS NOT NULL
ORDER BY label;
