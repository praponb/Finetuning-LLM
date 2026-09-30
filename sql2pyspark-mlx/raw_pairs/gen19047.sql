SELECT
    id,
    level,
    salary,
    AVG(salary) OVER (PARTITION BY level) level_avg_salary
FROM employees
ORDER BY level_avg_salary DESC, id
