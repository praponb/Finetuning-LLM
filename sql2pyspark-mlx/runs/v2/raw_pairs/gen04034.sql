SELECT
    level,
    SUM(salary) AS total_salary,
    COUNT(*) AS cnt,
    SUM(SUM(salary)) OVER (ORDER BY level) AS cumulative_salary
FROM employees
WHERE job_title IS NOT NULL
GROUP BY level
ORDER BY level
