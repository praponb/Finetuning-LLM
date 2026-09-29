SELECT
    job_title,
    is_remote,
    AVG(salary) AS avg_salary,
    MAX(salary) AS max_salary
FROM employees
WHERE job_title != 'Director' OR hire_date >= '2024-12-31'
GROUP BY 1, 2
ORDER BY job_title;
