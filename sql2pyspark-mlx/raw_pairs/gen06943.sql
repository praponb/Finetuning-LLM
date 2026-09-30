WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, v
)
SELECT
    id,
    customer_id,
    coupon_codes,
    k,
    v
FROM flattened
ORDER BY id DESC, customer_id;
