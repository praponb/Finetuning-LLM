SELECT
    job_title,
    level,
    AVG(salary) AS avg_salary,
    MAX(salary) - MIN(salary) AS salary_range
FROM employees
WHERE is_remote = TRUE AND is_remote AND level NOT IN ('L4', 'L1')
GROUP BY 1, 2
HAVING avg_salary > 86000
