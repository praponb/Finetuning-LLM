WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM carts
)
SELECT
    id,
    customer_id,
    created_at,
    code
FROM lines;
