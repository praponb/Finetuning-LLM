WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        value
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS value
)
SELECT
    id,
    created_at,
    value,
    CASE WHEN value RLIKE '^(VIP|BUNDLE)$' THEN 'CORE' WHEN value RLIKE '^(SPRING10|WELCOME5)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS value_zone
FROM exploded
ORDER BY value_zone DESC;
