WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS element
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM flattened
    WHERE effective_from >= DATE '2024-03-24'
      AND element <= 63
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(effective_from, 12) AS effective_from_plus_months
    FROM base
    WHERE days_since_effective_from >= 60
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        plan,
        element,
        days_since_effective_from,
        effective_from_plus_months,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY days_since_effective_from DESC NULLS FIRST) AS days_since_effective_from_cume_dist,
        MIN(days_since_effective_from) OVER (PARTITION BY plan) AS plan_min_days_since_effective_from,
        AVG(element) OVER (PARTITION BY YEAR(effective_from), plan) AS effective_from_year_avg_element
    FROM valid
),
kept_keys AS (
    -- Set intersection on keys
    SELECT plan
    FROM with_windows
    INTERSECT
    SELECT plan
    FROM price_tiers
    WHERE currency IN ('THB', 'USD')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.plan = k.plan
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', effective_from_plus_months) AS quarter_start,
    plan,
    ROUND(SUM(effective_from_year_avg_element) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS effective_from_year_avg_element_per_starter,
    MAX(days_since_effective_from_cume_dist) FILTER (WHERE plan = 'starter') AS starter_max_days_since_effective_from_cume_dist,
    COUNT(DISTINCT plan) AS distinct_plans,
    APPROX_PERCENTILE(days_since_effective_from_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS days_since_effective_from_cume_dist_quartiles
FROM verified
GROUP BY DATE_TRUNC('QUARTER', effective_from_plus_months), plan;
