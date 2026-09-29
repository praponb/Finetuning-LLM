WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END AS is_engineering
    FROM departments
    WHERE location IN ('Berlin', 'Bangkok', 'Singapore')
)
-- Multi-level aggregation
SELECT
    location,
    SUM(budget) AS total_budget,
    ROUND(AVG(budget), 2) AS avg_budget,
    COUNT(*) FILTER (WHERE dept_name = 'Finance') AS finance_rows
FROM base
GROUP BY location
ORDER BY total_budget DESC NULLS FIRST;
