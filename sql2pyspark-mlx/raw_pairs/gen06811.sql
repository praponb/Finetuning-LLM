WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS val
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        MIN(val) OVER (PARTITION BY plan, currency) AS plan_min_val,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_percent_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS FIRST) AS val_dense_rank
    FROM flattened
    WHERE val < 53
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(val_percent_rank, 0) AS val_percent_rank_filled
    FROM metrics
    WHERE plan_count_val <> 12
),
metrics_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        plan,
        plan_min_val,
        plan_count_val,
        val_percent_rank,
        val_dense_rank,
        val_percent_rank_filled,
        ROUND(val_dense_rank / SUM(val_dense_rank) OVER (PARTITION BY plan), 4) AS val_dense_rank_share
    FROM cleaned
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(val_percent_rank, 0) AS val_percent_rank_filled_2
    FROM metrics_price_tiers
    WHERE plan_count_val BETWEEN 10 AND 12
)
SELECT
    plan_min_val,
    plan_count_val,
    val_percent_rank,
    val_percent_rank_filled,
    val_dense_rank_share
FROM valid
ORDER BY val_percent_rank_filled DESC NULLS FIRST
