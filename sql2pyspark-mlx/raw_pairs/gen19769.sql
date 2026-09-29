SELECT DATE_TRUNC('MONTH', hire_date) AS hire_month, level, ROUND((100 * SUM(CASE WHEN level = 'L1' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l1
FROM employees
WHERE manager_id = 25
GROUP BY 1, 2
HAVING pct_l1 >= 49
ORDER BY 3
