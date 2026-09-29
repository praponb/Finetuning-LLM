WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, attr_value
)
SELECT
    id,
    created_at,
    attr_value
FROM flattened
ORDER BY created_at NULLS LAST;
