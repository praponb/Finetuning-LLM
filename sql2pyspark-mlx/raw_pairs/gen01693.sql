WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS tier_value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END AS is_enterprise,
        ROUND(tier_value * 1.1, 2) AS tier_value_adjusted
    FROM flattened
    WHERE currency IN ('USD', 'THB', 'EUR')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(tier_value, 0) AS tier_value_rounded
    FROM cleaned
    WHERE is_enterprise BETWEEN 29 AND 66
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        currency,
        tier_value,
        is_enterprise,
        tier_value_adjusted,
        SUBSTRING(currency, 1, 4) AS currency_prefix
    FROM valid
    WHERE tier_value BETWEEN 20 AND 48
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN tier_value_adjusted >= 82 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_adjusted_tier,
        plan,
        SUM(CASE WHEN plan = 'basic' THEN is_enterprise ELSE 0 END) AS basic_is_enterprise,
        SUM(is_enterprise) AS total_is_enterprise,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list_2,
        PERCENTILE_APPROX(is_enterprise, ARRAY(0.25, 0.5, 0.75)) AS is_enterprise_quartiles,
        COUNT(*) AS event_count
    FROM base
    GROUP BY GROUPING SETS ((CASE WHEN tier_value_adjusted >= 82 THEN 'LARGE' ELSE 'SMALL' END, plan), (CASE WHEN tier_value_adjusted >= 82 THEN 'LARGE' ELSE 'SMALL' END))
    HAVING COUNT(*) < 1000
)
SELECT
    tier_value_adjusted_tier,
    total_is_enterprise,
    plan_list,
    plan_list_2,
    is_enterprise_quartiles,
    event_count
FROM stats;
