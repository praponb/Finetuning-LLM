SELECT
    id,
    salary,
    job_title,
    hire_date
FROM employees
WHERE salary > (SELECT MAX(salary) FROM employees WHERE level NOT IN ('L4', 'L5'));
