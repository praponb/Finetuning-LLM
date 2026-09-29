WITH RecentDepartments AS (
    SELECT *
    FROM departments
    WHERE location = 'New York'
),
location_summary AS (
    SELECT location, AVG(budget) AS avg_budget
    FROM RecentDepartments
    GROUP BY location
)
SELECT *
FROM location_summary
WHERE avg_budget >= 3412000;
