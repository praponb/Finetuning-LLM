SELECT
    job_title,
    level,
    MAX(salary) - MIN(salary) salary_range,
    MIN(salary) min_salary,
    ROUND(STDDEV(salary), 2) stddev_salary
FROM employees
WHERE is_remote = TRUE
GROUP BY job_title, level
ORDER BY level, job_title
LIMIT 10;
