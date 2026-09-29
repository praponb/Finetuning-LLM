WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(plan) AS clean_plan,
        COALESCE(val, 0) AS val_filled
    FROM lines
    WHERE effective_from > DATE '2024-03-08'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        currency,
        val,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val DESC) AS val_dense_rank
    FROM base
    WHERE effective_from > '2024-03-12'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        currency,
        val,
        PERCENT_RANK() OVER w_recent AS val_dense_rank_percent_rank
    FROM scored
    WINDOW w_recent AS (ORDER BY val_dense_rank NULLS LAST)
)
SELECT val, val_dense_rank_percent_rank
FROM metrics
ORDER BY val DESC NULLS LAST, val_dense_rank_percent_rank
