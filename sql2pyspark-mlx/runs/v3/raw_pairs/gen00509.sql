SELECT
    id,
    hire_date,
    job_title,
    level
FROM employees
WHERE hire_date = (SELECT MIN(hire_date) FROM employees WHERE job_title = 'Analyst');
