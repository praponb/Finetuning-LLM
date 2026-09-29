SELECT
    id,
    job_title,
    salary,
    MAX(salary) OVER (PARTITION BY job_title) AS job_title_max_salary
FROM employees
ORDER BY 1 DESC;
