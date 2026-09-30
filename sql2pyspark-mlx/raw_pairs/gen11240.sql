WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS attr_key, attr_value
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    attr_key,
    ROW_NUMBER() OVER (PARTITION BY YEAR(created_at), attr_key ORDER BY created_at, id, attr_key) AS event_seq
FROM flattened;
