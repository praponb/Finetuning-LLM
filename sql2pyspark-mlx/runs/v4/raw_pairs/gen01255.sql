WITH filtered_departments AS (
    SELECT *, CASE WHEN location IN ('Tokyo', 'Bangkok') THEN 'Core' ELSE 'Other' END AS location_group
    FROM departments
    WHERE budget <= 3770000
),
summary AS (
    SELECT location, ROUND(STDDEV(budget), 2) AS stddev_budget, ROUND(AVG(budget), 2) AS avg_budget
    FROM filtered_departments
    GROUP BY location
)
SELECT *
FROM summary
WHERE stddev_budget > 27
ORDER BY 3 DESC, location
LIMIT 50
