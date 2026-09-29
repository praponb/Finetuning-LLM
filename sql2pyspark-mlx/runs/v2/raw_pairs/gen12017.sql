WITH filtered_departments AS (
    SELECT *
    FROM departments
    WHERE location IN ('Chiang Mai', 'Berlin', 'New York') AND budget <= 1285000
),
summary AS (
    SELECT location, COUNT(DISTINCT budget) AS unique_locations, ROUND(STDDEV(budget), 2) AS stddev_budget
    FROM filtered_departments
    GROUP BY location
)
SELECT location, unique_locations, stddev_budget
FROM summary
ORDER BY unique_locations
