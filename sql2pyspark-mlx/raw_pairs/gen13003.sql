SELECT CASE WHEN level IN ('L1', 'L2') THEN 'core' ELSE 'other' END AS level_group, level, SUM(salary) AS total_salary
FROM employees
WHERE level IN ('L3', 'L4', 'L1')
GROUP BY 1, 2
ORDER BY level
