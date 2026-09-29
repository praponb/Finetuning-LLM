SELECT
    manager_id,
    level,
    MAX(salary) - MIN(salary) salary_range,
    MIN(salary) min_salary,
    ROUND(STDDEV(salary), 2) stddev_salary
FROM employees
WHERE is_remote = TRUE
GROUP BY manager_id, level
ORDER BY level, manager_id
LIMIT 10;
