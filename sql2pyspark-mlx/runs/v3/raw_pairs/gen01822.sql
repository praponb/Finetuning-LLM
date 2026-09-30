SELECT
    location,
    MIN(budget) AS min_budget,
    MAX(budget) AS max_budget,
    SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END) AS engineering_count
FROM departments
GROUP BY location
ORDER BY min_budget DESC
