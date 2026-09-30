WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS val
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) AS val_dense_rank,
        RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS FIRST) AS val_rank
    FROM exploded
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        val,
        val_dense_rank,
        val_rank,
        PERCENT_RANK() OVER (ORDER BY val_dense_rank NULLS FIRST) AS val_dense_rank_percent_rank,
        RANK() OVER (PARTITION BY plan ORDER BY val_rank DESC) AS val_rank_rank,
        AVG(val) OVER (PARTITION BY plan) AS plan_avg_val,
        ROUND(val_dense_rank / SUM(val_dense_rank) OVER (PARTITION BY currency), 4) AS val_dense_rank_share
    FROM with_windows
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY val_dense_rank_percent_rank NULLS LAST) AS val_dense_rank_percent_rank_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY val_dense_rank_percent_rank NULLS LAST) AS val_dense_rank_percent_rank_cume_dist,
        SUM(plan_avg_val) OVER (PARTITION BY currency, plan) AS currency_sum_plan_avg_val
    FROM enriched
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(currency)) AS currency_title
    FROM scored
    WHERE val_rank_rank BETWEEN 3 AND 5
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, val_dense_rank_percent_rank_cume_dist - currency_sum_plan_avg_val AS val_dense_rank_percent_rank_cume_dist_minus_currency_sum_plan_avg_val
    FROM cleaned
    WHERE currency_sum_plan_avg_val = 346
)
SELECT
    plan,
    val,
    val_dense_rank,
    val_dense_rank_percent_rank,
    val_rank_rank,
    plan_avg_val,
    val_dense_rank_share,
    val_dense_rank_percent_rank_percent_rank,
    val_dense_rank_percent_rank_cume_dist
FROM filtered;
