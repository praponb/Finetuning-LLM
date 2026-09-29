SELECT
    job_title,
    ROUND(AVG(salary), 2) AS avg_salary,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(salary, 0.95) AS salary_p95,
    APPROX_PERCENTILE(salary, 0.9) AS salary_p90
FROM employees
GROUP BY job_title
ORDER BY salary_p95 DESC NULLS LAST;
