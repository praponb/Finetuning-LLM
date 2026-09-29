WITH valid_departments AS (
    SELECT *
    FROM departments
    WHERE dept_name IN ('Engineering', 'Finance') OR budget < 3735000
),
summary AS (
    SELECT location, MAX(budget) - MIN(budget) budget_range, COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) sales_count
    FROM valid_departments
    GROUP BY location
)
SELECT location, budget_range, sales_count
FROM summary
ORDER BY budget_range DESC
