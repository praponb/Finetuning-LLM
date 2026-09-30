WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS code
)
-- Multi-level aggregation
SELECT
    code,
    COUNT(*) FILTER (WHERE code = 'WELCOME5') AS welcome5_rows,
    SIZE(COLLECT_SET(code)) AS num_codes,
    MAX(created_at) AS last_created_at,
    MIN(created_at) AS first_created_at,
    COUNT(*) FILTER (WHERE code = 'VIP') AS vip_rows
FROM exploded
GROUP BY code
HAVING vip_rows < 1000
ORDER BY num_codes
