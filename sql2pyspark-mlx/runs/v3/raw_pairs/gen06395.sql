WITH valid_departments AS (
    SELECT *
    FROM departments
    WHERE location IN ('Berlin', 'Tokyo', 'Singapore')
),
departments_stats AS (
    SELECT location, MAX(budget) max_budget, MAX(budget) - MIN(budget) budget_range
    FROM valid_departments
    GROUP BY location
)
SELECT location, max_budget, budget_range
FROM departments_stats;
