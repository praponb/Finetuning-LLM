WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY MONTH(effective_from), plan ORDER BY tier_value DESC) AS tier_value_rank,
        RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_rank_2,
        DENSE_RANK() OVER (ORDER BY tier_value) AS tier_value_dense_rank
    FROM lines
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_SUB(effective_from, 61) AS effective_from_minus_61d,
        GREATEST(tier_value, tier_value_dense_rank) AS greatest_tier_value_tier_value_dense_rank
    FROM metrics
    WHERE effective_from > DATE '2024-05-03'
      AND tier_value <= 52
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        tier_value_rank,
        tier_value_rank_2,
        greatest_tier_value_tier_value_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value_rank_2 DESC NULLS LAST) AS tier_value_rank_2_cume_dist
    FROM cleaned
    WHERE plan IN ('starter', 'pro', 'enterprise')
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, DATE_ADD(effective_from, 26) AS effective_from_plus_26d
    FROM metrics_price_tiers
    WHERE tier_value_rank_2_cume_dist BETWEEN 0.6 AND 0.79
)
SELECT
    effective_from,
    tier_value_rank,
    tier_value_rank_2,
    greatest_tier_value_tier_value_dense_rank,
    tier_value_rank_2_cume_dist,
    effective_from_plus_26d
FROM valid;
