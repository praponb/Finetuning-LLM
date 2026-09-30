WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        value,
        EXPLODE(coupon_codes) AS value_2
    FROM lines
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(value_2, 'Unknown') AS value_2_filled,
        COALESCE(value_2, 'OTHER') AS value_2_filled_2
    FROM flattened
    WHERE value_2 IN ('VIP', 'SPRING10', 'WELCOME5')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(value_2)) AS value_2_title,
        TRIM(value_2) AS clean_value_2,
        COALESCE(value_2, 'N/A') AS value_2_clean
    FROM cleaned
    WHERE value_2 = 'WELCOME5'
),
filtered_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(value_2, 1, 3) AS value_2_prefix
    FROM filtered
    WHERE value_2 IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        value_2,
        value_2_filled,
        clean_value_2,
        value_2_clean,
        value_2_prefix,
        LOWER(value_2) AS value_2_lower,
        CASE WHEN value_2 = 'VIP' THEN 1 ELSE 0 END AS is_vip,
        COALESCE(value_2, 'NONE') AS value_2_clean_2
    FROM filtered_carts
    WHERE value_2 IN ('SPRING10', 'WELCOME5', 'BUNDLE')
),
valid_carts AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(is_vip * 0.85, 2) AS adjusted_is_vip,
        CASE WHEN value_2 = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle
    FROM valid
    WHERE value_2 IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN adjusted_is_vip >= 64 THEN 'LARGE' ELSE 'SMALL' END AS adjusted_is_vip_tier,
    value_2,
    COUNT(*) FILTER (WHERE value_2 = 'VIP') AS vip_rows,
    ROUND(AVG(is_vip), 2) AS avg_is_vip,
    COUNT(*) AS event_count,
    ROUND(SUM(is_vip) / NULLIF(COUNT(CASE WHEN value_2 = 'BUNDLE' THEN 1 END), 0), 2) AS is_vip_per_bundle,
    APPROX_PERCENTILE(is_vip, ARRAY(0.25, 0.5, 0.75)) AS is_vip_quartiles,
    SORT_ARRAY(COLLECT_SET(value_2)) AS value_2_list
FROM valid_carts
GROUP BY ROLLUP (CASE WHEN adjusted_is_vip >= 64 THEN 'LARGE' ELSE 'SMALL' END, value_2)
