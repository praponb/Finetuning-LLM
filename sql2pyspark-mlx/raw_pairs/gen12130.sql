WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        attr_key,
        value,
        tag
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS tag
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN attr_key = 'utm_source' THEN 1 ELSE 0 END AS is_utm_source,
        CONCAT(attr_key, ' - ', tag) AS attr_key_tag,
        COALESCE(attr_key, 'N/A') AS attr_key_clean
    FROM flattened
    WHERE tag IS NOT DISTINCT FROM 'WELCOME5'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN attr_key IN ('utm_source', 'weight_kg') THEN 'Tier 1' ELSE 'Other' END AS attr_key_group,
        ROUND(is_utm_source, 0) AS is_utm_source_rounded
    FROM cleaned
    WHERE attr_key IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(is_utm_source_rounded) OVER (PARTITION BY tag) AS tag_count_is_utm_source_rounded,
        PERCENT_RANK() OVER (PARTITION BY tag ORDER BY is_utm_source_rounded NULLS LAST) AS is_utm_source_rounded_percent_rank,
        MAX(is_utm_source) OVER (PARTITION BY tag, attr_key) AS tag_max_is_utm_source
    FROM valid
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tag_count_is_utm_source_rounded >= 18 THEN 'LARGE' ELSE 'SMALL' END AS tag_count_is_utm_source_rounded_tier,
    CASE WHEN is_utm_source_rounded >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_utm_source_rounded_tier,
    COUNT(*) FILTER (WHERE attr_key = 'gift') AS gift_rows,
    PERCENTILE_APPROX(is_utm_source_rounded, 0.9) AS is_utm_source_rounded_p90,
    SUM(is_utm_source_rounded) AS total_is_utm_source_rounded,
    APPROX_PERCENTILE(is_utm_source_rounded, ARRAY(0.25, 0.5, 0.75)) AS is_utm_source_rounded_quartiles,
    COUNT(DISTINCT tag) AS distinct_tags,
    GROUPING_ID() AS gid
FROM with_windows
GROUP BY GROUPING SETS ((CASE WHEN tag_count_is_utm_source_rounded >= 18 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN is_utm_source_rounded >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN tag_count_is_utm_source_rounded >= 18 THEN 'LARGE' ELSE 'SMALL' END))
