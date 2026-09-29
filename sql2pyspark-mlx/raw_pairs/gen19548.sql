WITH exploded AS (
    SELECT
        id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS k, value
),
flattened AS (
    SELECT
        id,
        k,
        value,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM exploded
),
filtered AS (
    SELECT *, INITCAP(TRIM(element)) AS element_title
    FROM flattened
    WHERE k IS DISTINCT FROM 'device'
),
valid AS (
    SELECT *, LOWER(k) AS k_lower
    FROM filtered
    WHERE k <> 'gift'
)
SELECT
    id,
    k,
    value,
    element,
    element_title,
    k_lower
FROM valid;
