SELECT
    id,
    level,
    job_title,
    salary,
    ROUND(salary / SUM(salary) OVER (PARTITION BY level, job_title), 3) AS salary_share
FROM employees
ORDER BY 2;
