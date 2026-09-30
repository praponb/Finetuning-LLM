WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(tier_value) OVER (PARTITION BY DATE_TRUNC('month', effective_from), plan) AS effective_from_period_count_tier_value,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_cume_dist
    FROM lines
    WHERE MONTH(effective_from) IN (2, 5, 12)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        effective_from,
        effective_from_period_count_tier_value,
        tier_value_cume_dist,
        CASE WHEN tier_value_cume_dist >= 0.71 THEN 'High' WHEN tier_value_cume_dist >= 0.33 THEN 'Medium' ELSE 'Low' END AS tier_value_cume_dist_level,
        effective_from_period_count_tier_value + tier_value AS effective_from_period_count_tier_value_plus_tier_value
    FROM with_windows
    WHERE plan = 'basic'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        plan,
        effective_from,
        effective_from_period_count_tier_value,
        tier_value_cume_dist_level,
        effective_from_period_count_tier_value_plus_tier_value,
        CUME_DIST() OVER (ORDER BY effective_from_period_count_tier_value_plus_tier_value) AS effective_from_period_count_tier_value_plus_tier_value_cume_dist
    FROM filtered
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        effective_from,
        tier_value_cume_dist_level,
        effective_from_period_count_tier_value_plus_tier_value,
        effective_from_period_count_tier_value_plus_tier_value_cume_dist,
        UPPER(plan) AS plan_upper,
        ADD_MONTHS(effective_from, 12) AS effective_from_plus_months
    FROM metrics
    WHERE effective_from >= TO_DATE('2024-06-07')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY effective_from_period_count_tier_value_plus_tier_value_cume_dist DESC NULLS LAST) AS effective_from_period_count_tier_value_plus_tier_value_cume_dist_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY effective_from_period_count_tier_value_plus_tier_value_cume_dist NULLS FIRST) AS effective_from_period_count_tier_value_plus_tier_value_cume_dist_percent_rank_2,
        MIN(effective_from_period_count_tier_value_plus_tier_value) OVER w_recent AS plan_min_effective_from_period_count_tier_value_plus_tier_value
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY plan)
)
-- Running and moving aggregates
SELECT
    plan,
    effective_from,
    effective_from_period_count_tier_value_plus_tier_value,
    plan_upper,
    plan_min_effective_from_period_count_tier_value_plus_tier_value,
    AVG(effective_from_period_count_tier_value_plus_tier_value_cume_dist) OVER (PARTITION BY DATE_TRUNC('month', effective_from_plus_months), plan) AS effective_from_plus_months_period_avg_effective_from_period_count_tier_value_plus_tier_value_cume_dist,
    AVG(effective_from_period_count_tier_value_plus_tier_value_cume_dist_percent_rank) OVER (PARTITION BY plan) AS plan_avg_effective_from_period_count_tier_value_plus_tier_value_cume_dist_percent_rank
FROM scored
