WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        INLINE(items)
    FROM carts
)
SELECT
    customer_id,
    coupon_codes,
    price,
    promo
FROM lines;
