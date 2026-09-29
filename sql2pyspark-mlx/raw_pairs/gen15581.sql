WITH exploded AS (
    SELECT
        id,
        customer_id,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS element
)
SELECT id
FROM exploded
ORDER BY id NULLS LAST;
