SELECT
    location,
    MAX(budget) - MIN(budget) AS budget_range,
    COUNT(CASE WHEN location = 'Berlin' THEN 1 END) AS berlin_count,
    SUM(CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END) AS hr_count
FROM departments
WHERE location IN ('Singapore', 'London') OR budget BETWEEN 1366000 AND 1446000
GROUP BY location
