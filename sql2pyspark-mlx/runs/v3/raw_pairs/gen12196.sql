SELECT
    level,
    ROUND((100 * SUM(CASE WHEN level = 'L1' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l1,
    COUNT(CASE WHEN level = 'L1' THEN 1 END) AS l1_count,
    MAX(salary) AS max_salary
FROM employees
GROUP BY level;
