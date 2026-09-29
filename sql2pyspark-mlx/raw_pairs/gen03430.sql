WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
with_windows AS (
    SELECT
        id,
        plan,
        DENSE_RANK() OVER (ORDER BY val DESC) AS val_dense_rank,
        MAX(val) OVER (PARTITION BY plan) AS plan_max_val
    FROM exploded
    WHERE currency IN ('EUR', 'THB')
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY plan_max_val DESC) AS plan_max_val_dense_rank,
        SUM(plan_max_val) OVER (PARTITION BY plan) AS plan_sum_plan_max_val,
        RANK() OVER w_recent AS plan_max_val_rank
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY plan ORDER BY plan_max_val DESC NULLS LAST)
),
scored_price_tiers AS (
    SELECT
        plan,
        val_dense_rank,
        plan_sum_plan_max_val,
        plan_max_val_rank,
        ROUND(plan_max_val_rank / SUM(plan_max_val_rank) OVER (PARTITION BY plan), 4) AS plan_max_val_rank_share,
        RANK() OVER (ORDER BY plan_max_val_rank) AS plan_max_val_rank_rank,
        ROUND(val_dense_rank / SUM(val_dense_rank) OVER (PARTITION BY plan), 4) AS val_dense_rank_share,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY plan_max_val_rank) AS plan_max_val_rank_dense_rank
    FROM scored
)
SELECT
    plan,
    APPROX_PERCENTILE(plan_max_val_rank_rank, ARRAY(0.25, 0.5, 0.75)) AS plan_max_val_rank_rank_quartiles,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN plan = 'pro' THEN 1 END) AS pro_count,
    PERCENTILE_APPROX(plan_max_val_rank_rank, 0.95) AS plan_max_val_rank_rank_p95
FROM scored_price_tiers
GROUP BY CUBE (plan)
ORDER BY row_count DESC
