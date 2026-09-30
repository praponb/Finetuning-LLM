WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, attr_value
),
exploded AS (
    SELECT
        id,
        k,
        attr_value,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM flattened
),
valid AS (
    SELECT
        *,
        CASE WHEN k IN ('weight_kg', 'utm_source') THEN 'Tier 1' ELSE 'Other' END AS k_group,
        LOWER(element) AS element_lower
    FROM exploded
    WHERE element IS NOT NULL
      AND element != 'WELCOME5'
)
SELECT
    id,
    k,
    attr_value,
    element,
    k_group,
    element_lower
FROM valid
ORDER BY k;
