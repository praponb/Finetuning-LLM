SELECT
    level,
    is_remote,
    SUM(salary) AS total_salary,
    ROUND((100 * SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l4,
    ROUND(STDDEV(salary), 2) AS stddev_salary
FROM employees
GROUP BY level, is_remote
HAVING AVG(salary) < 78000
ORDER BY pct_l4 DESC;
