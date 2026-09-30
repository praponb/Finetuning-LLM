SELECT
    location,
    COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END) AS finance_count,
    SUM(CASE WHEN location = 'Berlin' THEN budget ELSE 0 END) AS berlin_budget,
    MAX(budget) AS max_budget
FROM departments
WHERE location IN ('Chiang Mai', 'Tokyo', 'Singapore') AND budget BETWEEN 3469000 AND 3666000 AND budget > 2824000
GROUP BY location
HAVING MAX(budget) >= 1961000;
