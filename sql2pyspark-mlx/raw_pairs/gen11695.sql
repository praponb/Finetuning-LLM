WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), effective_from) / 365 AS years_since_effective_from
    FROM lines
    WHERE effective_from > DATE '2024-02-02'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(tier_value) OVER (PARTITION BY currency) AS currency_count_tier_value,
        CUME_DIST() OVER (ORDER BY years_since_effective_from DESC NULLS LAST) AS years_since_effective_from_cume_dist
    FROM base
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(currency_count_tier_value * 1.1, 2) AS currency_count_tier_value_adjusted
    FROM with_windows
    WHERE years_since_effective_from >= 43
)
-- Grouped metrics
SELECT
    currency,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    MAX(years_since_effective_from) FILTER (WHERE currency = 'THB') AS thb_max_years_since_effective_from,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN currency = 'EUR' THEN years_since_effective_from ELSE 0 END) AS eur_years_since_effective_from,
    SIZE(COLLECT_SET(currency)) AS num_currencies
FROM valid
GROUP BY GROUPING SETS ((currency, DATE_TRUNC('MONTH', effective_from)), (currency))
HAVING distinct_ids > 1
