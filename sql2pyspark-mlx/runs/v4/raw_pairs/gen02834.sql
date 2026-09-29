SELECT
    job_title,
    SUM(salary) AS total_salary,
    COUNT(*) AS cnt,
    SUM(SUM(salary)) OVER (ORDER BY job_title) AS cumulative_salary
FROM employees
GROUP BY job_title
