SELECT job_title, level
FROM employees
EXCEPT DISTINCT
SELECT job_title, level
FROM employees
WHERE salary > 66000;
