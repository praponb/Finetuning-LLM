WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.qty,
        line.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS line
        FROM carts
    ) li
)
SELECT
    id,
    customer_id,
    coupon_codes,
    qty,
    promo
FROM lines
ORDER BY customer_id DESC NULLS FIRST
