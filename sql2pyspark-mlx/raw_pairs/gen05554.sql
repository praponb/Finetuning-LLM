WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS key, attr_value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        key,
        attr_value,
        tag
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) x AS tag
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        attr_value,
        INITCAP(TRIM(key)) AS key_title
    FROM flattened
    WHERE key != 'device'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, WEEKOFYEAR(created_at) AS created_at_weekofyear
    FROM cleaned
    WHERE created_at > TIMESTAMP '2025-03-01 18:00:00'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, RANK() OVER (ORDER BY created_at_weekofyear DESC NULLS FIRST) AS created_at_weekofyear_rank
    FROM base
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    CASE WHEN created_at_weekofyear_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS created_at_weekofyear_rank_tier,
    CASE WHEN created_at_weekofyear >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END AS created_at_weekofyear_tier,
    COUNT(*) AS row_count,
    MAX(created_at) AS last_created_at,
    MIN(created_at) AS started_at
FROM metrics
GROUP BY hour_window, created_at_weekofyear_rank_tier, created_at_weekofyear_tier
ORDER BY row_count DESC NULLS FIRST;
