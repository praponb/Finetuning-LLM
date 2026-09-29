WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    id,
    currency,
    tier_value,
    SUM(tier_value) OVER (PARTITION BY currency) AS currency_sum_tier_value,
    MIN(tier_value) OVER w_running AS effective_from_period_min_tier_value,
    RANK() OVER (PARTITION BY MONTH(effective_from), currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_rank
FROM exploded
WINDOW w_running AS (PARTITION BY DATE_TRUNC('month', effective_from), currency);
