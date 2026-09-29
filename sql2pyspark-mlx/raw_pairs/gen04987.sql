WITH exploded AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, value
),
exploded_carts AS (
    SELECT
        id,
        value,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM exploded
),
valid AS (
    SELECT
        *,
        LOWER(element) AS element_lower,
        COALESCE(element, 'NONE') AS element_clean
    FROM exploded_carts
    WHERE element IS NOT NULL
),
filtered AS (
    SELECT
        id,
        value,
        element,
        element_lower,
        element_clean,
        TRIM(element) AS clean_element
    FROM valid
    WHERE element IN ('SPRING10', 'BUNDLE', 'VIP')
)
SELECT
    id,
    element,
    element_lower,
    element_clean
FROM filtered
ORDER BY element ASC NULLS FIRST;
