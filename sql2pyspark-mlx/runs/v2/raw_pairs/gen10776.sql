SELECT
    level,
    SUM(salary) AS total_salary,
    COUNT(*) AS cnt,
    SUM(SUM(salary)) OVER (ORDER BY level) AS cumulative_salary
FROM employees
WHERE salary > 113000
GROUP BY level
ORDER BY cumulative_salary DESC;
