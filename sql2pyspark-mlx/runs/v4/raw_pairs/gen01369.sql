WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(tag)) AS tag_title
    FROM lines
    WHERE tag IS NOT NULL
      AND tag NOT IN ('BUNDLE', 'VIP', 'WELCOME5')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(tag) AS tag_length,
        COALESCE(tag, 'NONE') AS tag_clean
    FROM filtered
    WHERE tag IN ('BUNDLE', 'VIP', 'WELCOME5')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        tag,
        tag_title,
        tag_length,
        FLOOR(tag_length) AS tag_length_whole
    FROM cleaned
    WHERE tag IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN tag_length >= 67 THEN 'High' WHEN tag_length >= 37 THEN 'Medium' ELSE 'Low' END AS tag_length_band
    FROM valid
    WHERE tag IS NOT NULL
),
filtered_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        tag,
        tag_length,
        tag_length_whole,
        tag_length_band,
        CAST(tag_length_whole AS STRING) AS tag_length_whole_str,
        tag_length * tag_length_whole AS tag_length_times_tag_length_whole
    FROM base
    WHERE tag = 'WELCOME5'
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN tag_length_times_tag_length_whole >= 83 THEN 'LARGE' ELSE 'SMALL' END AS tag_length_times_tag_length_whole_tier,
        PERCENTILE_APPROX(tag_length, 0.95) AS tag_length_p95,
        AVG(tag_length) FILTER (WHERE tag = 'SPRING10') AS spring10_avg_tag_length,
        PERCENTILE_APPROX(tag_length_times_tag_length_whole, 0.9) AS tag_length_times_tag_length_whole_p90,
        COUNT(*) AS cnt,
        SIZE(COLLECT_SET(tag)) AS num_tags
    FROM filtered_carts
    GROUP BY 1
)
SELECT
    tag_length_times_tag_length_whole_tier,
    tag_length_p95,
    spring10_avg_tag_length,
    tag_length_times_tag_length_whole_p90,
    cnt,
    num_tags
FROM totals;
