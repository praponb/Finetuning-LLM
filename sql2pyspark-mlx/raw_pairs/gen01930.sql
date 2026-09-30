WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN dept_name IN ('Engineering', 'Legal') THEN 'Core' ELSE 'Other' END AS dept_name_group,
        ROUND(budget * 0.85, 2) AS budget_scaled,
        COALESCE(dept_name, 'N/A') AS dept_name_filled
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IS DISTINCT FROM 'Tokyo'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_scaled, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        budget,
        budget_scaled,
        dedup_rank,
        AVG(budget) OVER (PARTITION BY dept_name, location ORDER BY dedup_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_budget,
        MIN(budget) OVER (PARTITION BY dept_name, location ORDER BY dedup_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_budget,
        COUNT(budget) OVER (PARTITION BY dept_name, location ORDER BY dedup_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_budget,
        FIRST_VALUE(budget_scaled, TRUE) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget_scaled
    FROM ranked
    WHERE budget_scaled >= 66
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id IN (19, 38)
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT dept_name, id
    FROM eligible
    EXCEPT ALL
    SELECT dept_name, id
    FROM departments
    WHERE location = 'Berlin'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    JOIN matched_keys k ON s.dept_name = k.dept_name AND s.id = k.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
        MIN(budget_scaled) OVER (PARTITION BY dept_name) AS dept_name_min_budget_scaled
    FROM reconciled
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN last4_count_budget >= 16 THEN 'HIGH' ELSE 'LOW' END AS last4_count_budget_tier,
        CASE WHEN dept_name_min_budget_scaled >= 60 THEN 'HIGH' ELSE 'LOW' END AS dept_name_min_budget_scaled_tier,
        PERCENTILE_APPROX(budget_share, 0.9) AS budget_share_p90,
        COUNT(*) FILTER (WHERE dept_name = 'Marketing') AS marketing_rows
    FROM with_windows
    GROUP BY last4_count_budget_tier, dept_name_min_budget_scaled_tier
    HAVING budget_share_p90 < 1000000
)
SELECT
    last4_count_budget_tier,
    dept_name_min_budget_scaled_tier,
    budget_share_p90
FROM grouped
ORDER BY dept_name_min_budget_scaled_tier DESC NULLS LAST;
