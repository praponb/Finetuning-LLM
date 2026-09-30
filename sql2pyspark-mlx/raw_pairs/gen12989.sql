WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS value
    FROM carts
)
SELECT value
FROM flattened
ORDER BY value NULLS LAST;
