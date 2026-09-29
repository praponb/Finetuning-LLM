SELECT
    id,
    manager_id,
    salary,
    ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 3) salary_share
FROM employees
WHERE level IN ('L3', 'L4', 'L1')
