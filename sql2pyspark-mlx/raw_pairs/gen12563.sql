WITH lines AS (
    SELECT
        id,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS key, value
)
SELECT
    id,
    coupon_codes,
    key
FROM lines
ORDER BY key DESC
