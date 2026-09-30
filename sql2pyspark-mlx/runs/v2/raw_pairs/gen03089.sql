WITH filtered_departments AS (
    SELECT *, UPPER(dept_name) AS dept_name_upper
    FROM departments
    WHERE budget BETWEEN 1631000 AND 2609000 AND location = 'London' AND location NOT IN ('Bangkok', 'Chiang Mai', 'Berlin')
),
summary AS (
    SELECT CASE WHEN budget >= 3966000 THEN 'high' WHEN budget >= 2046000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN location = 'Berlin' THEN budget ELSE 0 END) AS berlin_budget, COUNT(DISTINCT budget) AS unique_locations
    FROM filtered_departments
    GROUP BY CASE WHEN budget >= 3966000 THEN 'high' WHEN budget >= 2046000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
