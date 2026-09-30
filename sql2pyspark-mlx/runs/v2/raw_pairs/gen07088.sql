WITH src AS (
    SELECT *
    FROM departments
    WHERE location NOT IN ('Singapore', 'Berlin') AND budget > 2918000
),
summary AS (
    SELECT dept_name, COUNT(CASE WHEN location = 'Berlin' THEN 1 END) AS berlin_count, ROUND((100 * SUM(CASE WHEN location = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_new_york
    FROM src
    GROUP BY dept_name
)
SELECT *
FROM summary
