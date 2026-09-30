WITH FilteredDepartments AS (
    SELECT *
    FROM departments
    WHERE dept_name = 'Engineering' AND location IN ('New York', 'London')
),
location_summary AS (
    SELECT location, SUM(budget) AS total_budget
    FROM FilteredDepartments
    GROUP BY location
)
SELECT *
FROM location_summary
WHERE total_budget >= 14704000
ORDER BY location;
