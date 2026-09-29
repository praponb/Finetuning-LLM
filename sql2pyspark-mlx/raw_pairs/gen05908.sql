WITH flattened AS (
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
filtered AS (
    SELECT *, CASE WHEN code IN ('WELCOME5', 'BUNDLE') THEN 'Group A' ELSE 'Other' END AS code_group
    FROM flattened
    WHERE customer_id IS NOT NULL
      AND code IS NOT DISTINCT FROM 'SPRING10'
),
base AS (
    SELECT *, CASE WHEN code = 'VIP' THEN 1 ELSE 0 END AS is_vip
    FROM filtered
    WHERE code IS NOT DISTINCT FROM 'SPRING10'
),
filtered_carts AS (
    SELECT
        *,
        ROUND(is_vip * 0.85, 2) AS adjusted_is_vip,
        COALESCE(is_vip, 0) AS is_vip_filled
    FROM base
    WHERE customer_id IS NOT NULL
),
grouped AS (
    SELECT
        customer_id,
        code,
        COUNT_IF(code = 'VIP') AS vip_events,
        SIZE(COLLECT_SET(code)) AS num_codes,
        ROUND(SUM(adjusted_is_vip) / NULLIF(COUNT(CASE WHEN code = 'VIP' THEN 1 END), 0), 2) AS adjusted_is_vip_per_vip
    FROM filtered_carts
    GROUP BY 1, 2
)
SELECT
    s.customer_id,
    s.code,
    s.vip_events,
    s.num_codes,
    CASE WHEN code RLIKE '^(SPRING10|VIP)$' THEN 'EXTENDED' WHEN code RLIKE '^(BUNDLE|WELCOME5)$' THEN 'CORE' ELSE 'GROUP_A' END AS code_zone,
    (SELECT MAX(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS max_order_amount
FROM grouped s
ORDER BY customer_id DESC NULLS FIRST, vip_events DESC;
