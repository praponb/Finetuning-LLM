WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS val
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(val) OVER (PARTITION BY plan) AS plan_min_val,
        CUME_DIST() OVER w_part AS val_cume_dist
    FROM lines
    WINDOW w_part AS (ORDER BY val DESC NULLS LAST)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_grp AS val_cume_dist_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY val_cume_dist DESC NULLS FIRST) AS val_cume_dist_rank,
        DENSE_RANK() OVER w_recent AS plan_min_val_dense_rank
    FROM scored
    WINDOW w_grp AS (PARTITION BY plan ORDER BY val_cume_dist DESC), w_recent AS (PARTITION BY currency, plan ORDER BY plan_min_val NULLS LAST)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        val,
        plan_min_val,
        val_cume_dist,
        val_cume_dist_cume_dist,
        val_cume_dist_rank,
        plan_min_val_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY val_cume_dist_cume_dist DESC NULLS FIRST) AS val_cume_dist_cume_dist_cume_dist,
        COUNT(val_cume_dist) OVER (PARTITION BY plan) AS plan_count_val_cume_dist
    FROM with_windows
),
metrics_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(val_cume_dist_cume_dist) OVER (PARTITION BY currency) AS currency_count_val_cume_dist_cume_dist,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val_cume_dist_cume_dist_cume_dist) AS val_cume_dist_cume_dist_cume_dist_cume_dist,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY plan_min_val_dense_rank) AS plan_min_val_dense_rank_dense_rank
    FROM metrics
)
-- Grouped metrics
SELECT
    currency,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(plan_min_val_dense_rank_dense_rank, 0.5) AS plan_min_val_dense_rank_dense_rank_p50,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    PERCENTILE_APPROX(plan_min_val_dense_rank_dense_rank, 0.5) AS plan_min_val_dense_rank_dense_rank_p50_2
FROM metrics_price_tiers
GROUP BY currency
ORDER BY plan_min_val_dense_rank_dense_rank_p50_2 DESC;
