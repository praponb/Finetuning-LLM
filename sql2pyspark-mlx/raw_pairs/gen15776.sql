WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS key, value
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        key,
        EXPLODE(coupon_codes) AS tag
    FROM flattened
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN tag = 'VIP' THEN 1 ELSE 0 END AS is_vip,
        SUBSTRING(tag, 1, 3) AS tag_prefix
    FROM exploded
    WHERE tag IS NOT NULL
      AND key = 'gift'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        key,
        tag,
        is_vip,
        SUM(is_vip) OVER (PARTITION BY tag) AS tag_sum_is_vip,
        STDDEV_SAMP(is_vip) OVER (PARTITION BY key) AS key_stddev_samp_is_vip
    FROM cleaned
    WHERE tag IN ('WELCOME5', 'BUNDLE', 'VIP')
),
stats AS (
    -- Grouped metrics
    SELECT
        key,
        APPROX_PERCENTILE(tag_sum_is_vip, ARRAY(0.25, 0.5, 0.75)) AS tag_sum_is_vip_quartiles,
        MAX(tag_sum_is_vip) FILTER (WHERE key = 'gift') AS gift_max_tag_sum_is_vip,
        COUNT_IF(tag = 'SPRING10') AS spring10_events,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list
    FROM enriched
    GROUP BY 1
    HAVING gift_max_tag_sum_is_vip > 0
)
SELECT
    key,
    tag_sum_is_vip_quartiles,
    gift_max_tag_sum_is_vip,
    spring10_events,
    key_list,
    CASE WHEN key RLIKE '^(device|utm_source)$' THEN 'CORE' WHEN key RLIKE '^(gift|weight_kg)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS key_zone
FROM stats
ORDER BY gift_max_tag_sum_is_vip DESC;
