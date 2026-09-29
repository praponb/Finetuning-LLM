WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
)
SELECT value
FROM exploded;
