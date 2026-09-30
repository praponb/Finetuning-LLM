WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS tier_value
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM lines
    EXCEPT
    SELECT id, plan
    FROM price_tiers
    WHERE effective_from BETWEEN '2023-11-01' AND '2024-05-26'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN matched_keys k ON s.id = k.id AND s.plan = k.plan
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(tier_value) OVER w_part AS plan_sum_tier_value,
        DENSE_RANK() OVER w_running AS tier_value_dense_rank,
        MAX(tier_value) OVER w_part AS plan_max_tier_value
    FROM verified
    WINDOW w_part AS (PARTITION BY plan), w_running AS (PARTITION BY DATE_TRUNC('WEEK', effective_from), plan ORDER BY tier_value ASC NULLS FIRST)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        plan_sum_tier_value - tier_value AS plan_sum_tier_value_minus_tier_value,
        CASE WHEN tier_value_dense_rank >= 8 THEN 'High' WHEN tier_value_dense_rank >= 2 THEN 'Medium' ELSE 'Low' END AS tier_value_dense_rank_band
    FROM enriched
    WHERE effective_from > TO_DATE('2024-03-21')
),
cleaned_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(plan_sum_tier_value_minus_tier_value * 1.07, 2) AS adjusted_plan_sum_tier_value_minus_tier_value,
        plan_max_tier_value - tier_value_dense_rank AS plan_max_tier_value_minus_tier_value_dense_rank
    FROM cleaned
    WHERE effective_from > DATE '2024-03-26'
      AND tier_value_dense_rank < 6
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(adjusted_plan_sum_tier_value_minus_tier_value) OVER w_grp AS plan_min_adjusted_plan_sum_tier_value_minus_tier_value,
        MAX(plan_max_tier_value_minus_tier_value_dense_rank) OVER w_grp AS plan_max_plan_max_tier_value_minus_tier_value_dense_rank,
        CUME_DIST() OVER w_recent AS plan_max_tier_value_cume_dist
    FROM cleaned_price_tiers
    WINDOW w_grp AS (PARTITION BY plan), w_recent AS (ORDER BY plan_max_tier_value DESC NULLS LAST)
)
SELECT
    id,
    plan,
    plan_sum_tier_value,
    tier_value_dense_rank,
    plan_max_tier_value,
    plan_sum_tier_value_minus_tier_value,
    tier_value_dense_rank_band,
    adjusted_plan_sum_tier_value_minus_tier_value,
    plan_max_tier_value_minus_tier_value_dense_rank
FROM scored
ORDER BY tier_value_dense_rank DESC
