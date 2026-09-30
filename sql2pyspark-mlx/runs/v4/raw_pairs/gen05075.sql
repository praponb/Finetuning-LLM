SELECT
    dept_name,
    location,
    MAX(budget) - MIN(budget) AS budget_range,
    MAX(budget) AS max_budget
FROM departments
WHERE location IS NOT NULL AND location IN ('Berlin', 'Chiang Mai')
GROUP BY dept_name, location
HAVING COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) < 1;
