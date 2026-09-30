WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        item.product_id,
        item.price
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) e AS item
)
SELECT
    id,
    coupon_codes,
    product_id
FROM exploded
ORDER BY product_id DESC NULLS FIRST, id
