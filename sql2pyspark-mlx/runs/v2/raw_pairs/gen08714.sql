SELECT
    location,
    dept_name,
    ROUND((100 * SUM(CASE WHEN location = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_chiang_mai,
    COUNT(budget) AS budget_count
FROM departments
GROUP BY CUBE(location, dept_name)
ORDER BY dept_name;
