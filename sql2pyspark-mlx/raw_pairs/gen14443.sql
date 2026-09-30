WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN budget >= 4307000 THEN 'High' WHEN budget >= 1168000 THEN 'Medium' ELSE 'Low' END AS budget_tier,
        CASE WHEN budget >= 4403000 THEN 'High' WHEN budget >= 1445000 THEN 'Medium' ELSE 'Low' END AS budget_level
    FROM departments
    WHERE location IS NOT NULL
)
SELECT dept_name, location
FROM filtered
EXCEPT DISTINCT
SELECT dept_name, location
FROM departments
WHERE location IN ('Singapore', 'New York');
