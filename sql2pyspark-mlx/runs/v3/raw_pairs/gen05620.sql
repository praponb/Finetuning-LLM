SELECT
    id,
    salary,
    job_title,
    ROW_NUMBER() OVER (ORDER BY salary, id) AS rn,
    AVG(salary) OVER (PARTITION BY job_title) AS job_title_avg_salary
FROM employees
WHERE job_title IS NOT NULL
ORDER BY job_title_avg_salary DESC;
