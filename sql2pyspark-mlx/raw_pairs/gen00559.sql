WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS tag
)
SELECT
    id,
    created_at,
    tag,
    CASE WHEN tag RLIKE '^(VIP|SPRING10)$' THEN 'PRIORITY' WHEN tag RLIKE '^(BUNDLE|WELCOME5)$' THEN 'GROUP_B' ELSE 'CORE' END AS tag_zone
FROM lines
ORDER BY created_at NULLS LAST;
