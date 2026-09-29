WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS tier_value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(tier_value) OVER (PARTITION BY plan) AS plan_sum_tier_value,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value ASC) AS tier_value_dense_rank
    FROM lines
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(plan_sum_tier_value) AS plan_sum_tier_value_whole,
        CASE WHEN tier_value_dense_rank >= 9 THEN 'High' WHEN tier_value_dense_rank >= 3 THEN 'Medium' ELSE 'Low' END AS tier_value_dense_rank_band
    FROM scored
    WHERE effective_from >= DATE '2024-02-01'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(plan_sum_tier_value, plan_sum_tier_value_whole) AS least_plan_sum_tier_value_plan_sum_tier_value_whole
    FROM filtered
    WHERE effective_from > TO_DATE('2024-05-05')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        plan_sum_tier_value_whole,
        MIN(plan_sum_tier_value_whole) OVER w_grp AS plan_min_plan_sum_tier_value_whole,
        STDDEV_SAMP(plan_sum_tier_value) OVER w_grp AS plan_stddev_samp_plan_sum_tier_value,
        RANK() OVER w_grp_2 AS least_plan_sum_tier_value_plan_sum_tier_value_whole_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_sum_tier_value DESC NULLS FIRST) AS plan_sum_tier_value_cume_dist,
        RANK() OVER w_ordered AS plan_sum_tier_value_whole_rank
    FROM base
    WINDOW w_grp AS (PARTITION BY plan), w_grp_2 AS (PARTITION BY plan ORDER BY least_plan_sum_tier_value_plan_sum_tier_value_whole DESC), w_ordered AS (PARTITION BY plan ORDER BY plan_sum_tier_value_whole ASC)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        plan_stddev_samp_plan_sum_tier_value - plan_sum_tier_value_cume_dist AS plan_stddev_samp_plan_sum_tier_value_minus_plan_sum_tier_value_cume_dist,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM with_windows
    WHERE effective_from > DATE '2024-03-01'
)
SELECT
    id,
    effective_from,
    plan_sum_tier_value_whole,
    least_plan_sum_tier_value_plan_sum_tier_value_whole_rank,
    plan_sum_tier_value_whole_rank,
    plan_stddev_samp_plan_sum_tier_value_minus_plan_sum_tier_value_cume_dist,
    days_since_effective_from
FROM valid
ORDER BY days_since_effective_from ASC;
