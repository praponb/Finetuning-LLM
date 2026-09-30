WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS val
),
filtered AS (
    SELECT *, COALESCE(val, 0) AS val_filled
    FROM exploded
    WHERE effective_from BETWEEN '2023-07-01' AND '2025-05-01'
),
scored AS (
    SELECT *, CUME_DIST() OVER (ORDER BY val NULLS LAST) AS val_cume_dist
    FROM filtered
    WHERE plan IN ('basic', 'starter', 'pro')
),
with_windows AS (
    SELECT
        id,
        plan,
        effective_from,
        val_filled,
        val_cume_dist,
        AVG(val_filled) OVER (PARTITION BY plan) AS plan_avg_val_filled,
        RANK() OVER (PARTITION BY plan ORDER BY val_filled DESC NULLS FIRST) AS val_filled_rank,
        SUM(val_cume_dist) OVER (PARTITION BY plan) AS plan_sum_val_cume_dist
    FROM scored
),
metrics AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY MONTH(effective_from), plan ORDER BY plan_sum_val_cume_dist DESC) AS plan_sum_val_cume_dist_rank,
        SUM(val_cume_dist) OVER (PARTITION BY plan) AS plan_sum_val_cume_dist_2,
        COUNT(val_cume_dist) OVER (PARTITION BY plan) AS plan_count_val_cume_dist
    FROM with_windows
),
base AS (
    SELECT *, LEAST(plan_sum_val_cume_dist_rank, val_filled_rank) AS least_plan_sum_val_cume_dist_rank_val_filled_rank
    FROM metrics
    WHERE effective_from > DATE '2024-02-19'
),
filtered_price_tiers AS (
    SELECT *, LEAST(val_cume_dist, plan_count_val_cume_dist) AS least_val_cume_dist_plan_count_val_cume_dist
    FROM base
    WHERE least_plan_sum_val_cume_dist_rank_val_filled_rank BETWEEN 73 AND 76
)
SELECT
    id,
    plan,
    val_cume_dist,
    plan_avg_val_filled,
    val_filled_rank,
    plan_sum_val_cume_dist,
    plan_sum_val_cume_dist_rank,
    plan_sum_val_cume_dist_2,
    least_plan_sum_val_cume_dist_rank_val_filled_rank
FROM filtered_price_tiers;
