WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS element
)
SELECT
    id,
    created_at,
    element,
    CASE WHEN element RLIKE '^(VIP|WELCOME5)$' THEN 'CORE' WHEN element RLIKE '^(SPRING10|BUNDLE)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS element_zone
FROM flattened
ORDER BY element_zone DESC;
