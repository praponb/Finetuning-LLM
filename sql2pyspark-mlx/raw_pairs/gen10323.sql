WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
lines AS (
    SELECT
        id,
        customer_id,
        product_id,
        price,
        promo,
        element
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS element
),
with_dims AS (
    SELECT
        e.*,
        c.age,
        c.email,
        c.city
    FROM lines e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
matched_keys AS (
    SELECT customer_id
    FROM with_dims
    INTERSECT
    SELECT customer_id
    FROM carts
    WHERE created_at < '2024-03-02'
),
kept_rows AS (
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
)
SELECT
    s.id,
    s.customer_id,
    s.product_id,
    s.price,
    s.element,
    s.age,
    s.city,
    (SELECT AVG(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM kept_rows s
ORDER BY avg_subscription_monthly_fee DESC;
