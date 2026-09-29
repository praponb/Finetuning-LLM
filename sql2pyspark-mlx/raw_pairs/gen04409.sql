WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
enriched AS (
    -- Ranking and gap features
    SELECT
        plan,
        effective_from,
        currency,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) AS val_percent_rank
    FROM flattened
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(currency, 1, 2) AS currency_prefix,
        ROUND(val_percent_rank * 0.85, 2) AS adjusted_val_percent_rank
    FROM enriched
    WHERE effective_from >= TO_DATE('2024-03-24')
),
metrics AS (
    -- Running and moving aggregates
    SELECT *, AVG(adjusted_val_percent_rank) OVER (PARTITION BY plan) AS plan_avg_adjusted_val_percent_rank
    FROM valid
),
valid_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(adjusted_val_percent_rank, 0) AS adjusted_val_percent_rank_filled,
        ROUND(adjusted_val_percent_rank, 1) AS adjusted_val_percent_rank_rounded
    FROM metrics
    WHERE effective_from >= DATE '2024-05-07'
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    plan,
    measure,
    metric_value
FROM (
    SELECT
        plan,
        adjusted_val_percent_rank_rounded,
        adjusted_val_percent_rank_filled
    FROM valid_price_tiers
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (adjusted_val_percent_rank_rounded, adjusted_val_percent_rank_filled)
);
