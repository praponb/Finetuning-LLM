SELECT
    is_remote,
    ROUND(STDDEV(salary), 2) AS stddev_salary,
    ROUND((100 * SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l4,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY is_remote
ORDER BY pct_l4 DESC, is_remote
LIMIT 50
