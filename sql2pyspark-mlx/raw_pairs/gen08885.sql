WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) tier_value_percent_rank,
        AVG(tier_value) OVER w_recent effective_from_period_avg_tier_value,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST) tier_value_percent_rank_2,
        DENSE_RANK() OVER w_part tier_value_dense_rank
    FROM exploded
    WINDOW w_recent AS (PARTITION BY DATE_TRUNC('week', effective_from), currency), w_part AS (PARTITION BY currency ORDER BY tier_value NULLS FIRST)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(plan) plan_length
    FROM metrics
    WHERE effective_from >= TO_DATE('2024-02-09')
      AND tier_value_dense_rank >= 6
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        tier_value,
        tier_value_percent_rank,
        tier_value_percent_rank_2,
        tier_value_dense_rank,
        ROUND(tier_value_dense_rank / SUM(tier_value_dense_rank) OVER (PARTITION BY plan), 4) tier_value_dense_rank_share,
        CUME_DIST() OVER (PARTITION BY currency, plan ORDER BY effective_from_period_avg_tier_value DESC) effective_from_period_avg_tier_value_cume_dist,
        MIN(plan_length) OVER (PARTITION BY DATE_TRUNC('MONTH', effective_from), currency) effective_from_period_min_plan_length
    FROM filtered
),
report AS (
    SELECT
        tier_value_percent_rank,
        tier_value_percent_rank_2,
        tier_value_dense_rank,
        tier_value_dense_rank_share,
        effective_from_period_min_plan_length
    FROM metrics_price_tiers
    ORDER BY effective_from_period_min_plan_length, tier_value_percent_rank_2 DESC
)
SELECT
    tier_value_percent_rank,
    tier_value_percent_rank_2,
    tier_value_dense_rank,
    tier_value_dense_rank_share,
    effective_from_period_min_plan_length
FROM report
ORDER BY tier_value_dense_rank DESC NULLS LAST, tier_value_percent_rank
