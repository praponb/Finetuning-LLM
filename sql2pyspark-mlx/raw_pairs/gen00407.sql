WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN currency IN ('USD', 'EUR') THEN 'Core' ELSE 'Other' END currency_group
    FROM lines
    WHERE tier_value < 22
),
base_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(tier_value AS STRING) tier_value_num
    FROM base
    WHERE currency IN ('EUR', 'THB')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(tier_value * 0.85, 2) adjusted_tier_value,
        DATE_TRUNC('month', effective_from) effective_from_period
    FROM base_price_tiers
    WHERE effective_from > TO_DATE('2024-01-25')
      AND tier_value > 31
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(tier_value) OVER (PARTITION BY currency ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) tier_value_count_90d,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY adjusted_tier_value DESC) adjusted_tier_value_dense_rank,
        RANK() OVER (PARTITION BY currency ORDER BY adjusted_tier_value DESC) adjusted_tier_value_rank,
        MAX(adjusted_tier_value) OVER (PARTITION BY currency ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) adjusted_tier_value_max_30d
    FROM valid
)
-- Grouped metrics
SELECT
    CASE WHEN adjusted_tier_value_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS adjusted_tier_value_dense_rank_tier,
    DATE_TRUNC('MONTH', effective_from_period) AS activity_month,
    MAX(adjusted_tier_value_dense_rank) FILTER (WHERE currency = 'EUR') AS eur_max_adjusted_tier_value_dense_rank,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
    COUNT_IF(currency = 'USD') AS usd_events,
    MIN(effective_from_period) AS started_at
FROM scored
GROUP BY CASE WHEN adjusted_tier_value_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', effective_from_period)
