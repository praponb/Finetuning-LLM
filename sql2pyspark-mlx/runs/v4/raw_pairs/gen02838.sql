WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(budget, 0) AS budget_filled
    FROM departments
    WHERE location IN ('Chiang Mai', 'London', 'Tokyo')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        location,
        budget_filled,
        budget_filled + budget AS budget_filled_plus_budget,
        COALESCE(location, 'UNKNOWN') AS resolved_location
    FROM valid
    WHERE location IN ('Bangkok', 'London', 'Chiang Mai')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(budget_filled) OVER (PARTITION BY location ORDER BY budget_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_filled,
        LEAD(budget_filled, 1, 0) OVER (ORDER BY budget_filled, id) AS next_budget_filled,
        SUM(budget_filled) OVER (ORDER BY budget_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_filled
    FROM filtered
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY dept_name ORDER BY budget_filled DESC, id) AS budget_filled_bucket,
        LAST_VALUE(budget_filled) IGNORE NULLS OVER (PARTITION BY location ORDER BY budget_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget_filled,
        LEAD(budget_filled_plus_budget, 1) OVER (PARTITION BY location, dept_name ORDER BY cumulative_budget_filled, id) AS next_budget_filled_plus_budget,
        ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget_filled_plus_budget DESC, id) AS row_seq
    FROM metrics
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(running_budget_filled * 1.07, 2) AS running_budget_filled_scaled,
        budget_filled_plus_budget / NULLIF(next_budget_filled, 0) AS budget_filled_plus_budget_per_next_budget_filled,
        COALESCE(location, 'NONE') AS resolved_location_2
    FROM enriched
    WHERE location IS NOT NULL
)
-- Grouped metrics
SELECT
    dept_name,
    location,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(next_budget_filled, 0.5) AS next_budget_filled_p50,
    SIZE(COLLECT_SET(location)) AS num_locations,
    ROUND(SUM(budget_filled_plus_budget) / NULLIF(COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END), 0), 2) AS budget_filled_plus_budget_per_finance,
    COUNT(*) AS row_count,
    ROUND(AVG(cumulative_budget_filled), 2) AS avg_cumulative_budget_filled
FROM cleaned
GROUP BY 1, 2
HAVING next_budget_filled_p50 >= 0
ORDER BY row_count NULLS FIRST
