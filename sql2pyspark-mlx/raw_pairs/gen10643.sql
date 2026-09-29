WITH latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        dept_name,
        budget
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS dedup_rank
        FROM departments
    ) ranked
    WHERE dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    dept_name,
    CASE WHEN budget >= 4086000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END) AS finance_count,
    ROUND(AVG(budget), 2) AS avg_budget,
    COUNT(*) AS cnt,
    STDDEV_SAMP(budget) AS budget_stddev,
    COUNT(*) FILTER (WHERE dept_name = 'HR') AS hr_rows,
    MIN(budget) AS min_budget
FROM latest
GROUP BY GROUPING SETS ((dept_name, CASE WHEN budget >= 4086000 THEN 'HIGH' ELSE 'LOW' END), (dept_name), ())
ORDER BY avg_budget DESC NULLS FIRST
