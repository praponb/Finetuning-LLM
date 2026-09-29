WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS val
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, PERCENT_RANK() OVER (ORDER BY val DESC) AS val_percent_rank
    FROM lines
    WHERE currency IN ('THB', 'USD', 'EUR')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(plan, 1, 4) AS plan_prefix
    FROM with_windows
    WHERE plan IN ('starter', 'pro')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(val_percent_rank) OVER (PARTITION BY currency, plan) AS currency_avg_val_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_percent_rank_2,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_percent_rank DESC NULLS FIRST) AS val_percent_rank_percent_rank
    FROM filtered
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        val,
        plan_prefix,
        currency_avg_val_percent_rank,
        val_percent_rank_2,
        val_percent_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val_percent_rank) AS val_percent_rank_percent_rank_2,
        AVG(val_percent_rank_percent_rank) OVER (PARTITION BY plan) AS plan_avg_val_percent_rank_percent_rank,
        RANK() OVER (PARTITION BY plan ORDER BY val NULLS FIRST) AS val_rank
    FROM scored
)
SELECT
    id,
    val,
    plan_prefix,
    val_percent_rank_2,
    val_percent_rank_percent_rank_2,
    plan_avg_val_percent_rank_percent_rank
FROM enriched;
