WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
),
metrics AS (
    SELECT
        plan,
        val,
        RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS FIRST) AS val_rank,
        PERCENT_RANK() OVER (PARTITION BY MONTH(effective_from), plan ORDER BY val NULLS LAST) AS val_percent_rank,
        CUME_DIST() OVER (ORDER BY val DESC NULLS FIRST) AS val_cume_dist
    FROM lines
    WHERE MONTH(effective_from) IN (2, 9, 10)
),
filtered AS (
    SELECT
        *,
        GREATEST(val_cume_dist, val) AS greatest_val_cume_dist_val,
        COALESCE(val, 0) AS val_filled
    FROM metrics
    WHERE val_cume_dist BETWEEN 0.52 AND 0.67
),
metrics_price_tiers AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY greatest_val_cume_dist_val NULLS LAST) AS greatest_val_cume_dist_val_dense_rank,
        MIN(val_filled) OVER (PARTITION BY plan) AS plan_min_val_filled,
        SUM(greatest_val_cume_dist_val) OVER (PARTITION BY plan) AS plan_sum_greatest_val_cume_dist_val,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val_rank NULLS FIRST) AS val_rank_dense_rank
    FROM filtered
),
cleaned AS (
    SELECT
        plan,
        val,
        val_cume_dist,
        greatest_val_cume_dist_val,
        greatest_val_cume_dist_val_dense_rank,
        plan_min_val_filled,
        ROUND(plan_min_val_filled * 0.9, 2) AS adjusted_plan_min_val_filled,
        CASE WHEN plan IN ('enterprise', 'pro') THEN 'Tier 1' ELSE 'Other' END AS plan_group
    FROM metrics_price_tiers
    WHERE plan_sum_greatest_val_cume_dist_val < 197
)
SELECT *
FROM (
    SELECT
        plan,
        greatest_val_cume_dist_val,
        adjusted_plan_min_val_filled,
        val_cume_dist
    FROM cleaned
) src
UNPIVOT (
    value FOR metric_name IN (greatest_val_cume_dist_val AS GREATEST_VAL_CUME_DIST_VAL, adjusted_plan_min_val_filled, val_cume_dist)
) unp;
