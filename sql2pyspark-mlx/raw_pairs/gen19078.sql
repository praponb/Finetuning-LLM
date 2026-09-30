SELECT
    job_title,
    SUM(salary) AS total_salary,
    MAX(salary) - MIN(salary) AS salary_range,
    COUNT(*) AS total_count
FROM employees
GROUP BY job_title;
