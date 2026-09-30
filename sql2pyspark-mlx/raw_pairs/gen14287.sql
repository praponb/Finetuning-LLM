SELECT
    level,
    ROUND((100 * SUM(CASE WHEN job_title = 'Analyst' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_analyst,
    MIN(salary) AS min_salary,
    SUM(salary) AS total_salary
FROM employees
WHERE UPPER(full_name) LIKE '%IM %'
GROUP BY level
