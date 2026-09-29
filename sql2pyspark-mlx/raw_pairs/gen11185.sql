WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS tier_value
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN currency = 'USD' THEN 1 ELSE 0 END AS is_usd,
        ROUND(tier_value, 0) AS tier_value_rounded
    FROM lines
    WHERE currency = 'EUR'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_hist AS is_usd_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist,
        SUM(tier_value_rounded) OVER (PARTITION BY currency) AS currency_sum_tier_value_rounded,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY is_usd NULLS FIRST) AS is_usd_percent_rank
    FROM base
    WHERE tier_value_rounded <= 30
    WINDOW w_hist AS (PARTITION BY currency ORDER BY is_usd NULLS LAST)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN tier_value_rounded >= 72 THEN 'High' WHEN tier_value_rounded >= 37 THEN 'Medium' ELSE 'Low' END AS tier_value_rounded_tier,
        SUBSTRING(currency, 1, 4) AS currency_prefix
    FROM with_windows
    WHERE tier_value = 31
)
-- Ranking and gap features
SELECT
    id,
    tier_value,
    is_usd,
    tier_value_rounded,
    is_usd_rank,
    tier_value_cume_dist,
    is_usd_percent_rank,
    tier_value_rounded_tier,
    RANK() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_rank,
    COUNT(tier_value_cume_dist) OVER w_recent AS currency_count_tier_value_cume_dist,
    AVG(tier_value_cume_dist) OVER w_recent AS currency_avg_tier_value_cume_dist
FROM cleaned
WINDOW w_recent AS (PARTITION BY currency);
