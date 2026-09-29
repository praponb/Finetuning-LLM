WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, value
),
base AS (
    SELECT
        *,
        ARRAY_CONTAINS(coupon_codes, 'VIP') AS is_vip,
        INITCAP(TRIM(attr_key)) AS attr_key_title
    FROM lines
    WHERE created_at > TO_TIMESTAMP('2025-03-01 15:00:00')
      AND attr_key IS NOT NULL
),
valid AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        value,
        is_vip,
        attr_key_title,
        CAST(created_at AS DATE) AS created_at_day,
        CASE WHEN is_vip THEN 'Yes' ELSE 'No' END AS is_vip_label
    FROM base
    WHERE created_at < '2025-01-01'
),
base_carts AS (
    SELECT
        *,
        CAST(is_vip AS INT) AS is_vip_flag,
        DATEDIFF(CURRENT_DATE(), created_at_day) AS days_since_created_at_day
    FROM valid
    WHERE created_at >= DATE '2025-03-02'
      AND attr_key IS NOT NULL
),
stats AS (
    SELECT
        CASE WHEN days_since_created_at_day >= 83 THEN 'LARGE' ELSE 'SMALL' END AS days_since_created_at_day_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(created_at) AS first_seen,
        SUM(CASE WHEN attr_key = 'device' THEN is_vip_flag ELSE 0 END) AS device_is_vip_flag,
        COUNT(*) FILTER (WHERE attr_key = 'weight_kg') AS weight_kg_rows,
        COUNT(*) FILTER (WHERE attr_key = 'device') AS device_rows,
        MIN(days_since_created_at_day) AS min_days_since_created_at_day
    FROM base_carts
    GROUP BY CASE WHEN days_since_created_at_day >= 83 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    days_since_created_at_day_tier,
    distinct_ids,
    device_is_vip_flag,
    device_rows
FROM stats;
