WITH recent_departments AS (
    SELECT *
    FROM departments
    WHERE budget <= 3549000
),
LocationSummary AS (
    SELECT location, SUM(CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END) AS hr_count, COUNT(CASE WHEN location = 'Berlin' THEN 1 END) AS berlin_count
    FROM recent_departments
    GROUP BY location
)
SELECT location, hr_count, berlin_count
FROM LocationSummary
ORDER BY location;
