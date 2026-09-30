WITH lines AS (
    SELECT
        id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, attr_value
),
valid AS (
    SELECT *, CASE WHEN k IN ('device', 'weight_kg') THEN 'Group A' ELSE 'Other' END AS k_group
    FROM lines
    WHERE k = 'weight_kg'
),
filtered AS (
    SELECT
        *,
        SORT_ARRAY(coupon_codes) AS sorted_coupon_codes,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM valid
    WHERE k = 'device'
),
valid_carts AS (
    SELECT
        id,
        coupon_codes,
        k,
        attr_value,
        sorted_coupon_codes,
        CASE WHEN k = 'gift' THEN 1 ELSE 0 END AS is_gift
    FROM filtered
    WHERE SIZE(coupon_codes) > 1
)
SELECT
    coupon_codes,
    k,
    attr_value,
    CASE WHEN k RLIKE '^(utm_source|device)$' THEN 'CORE' WHEN k RLIKE '^(gift|weight_kg)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS k_zone
FROM valid_carts
ORDER BY k DESC
