SELECT
    id,
    level,
    salary,
    ROUND(salary / SUM(salary) OVER (PARTITION BY level), 3) AS salary_share
FROM employees
WHERE level = 'L2';
