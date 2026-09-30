WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, value
)
SELECT
    s.id,
    s.customer_id,
    s.coupon_codes,
    s.key,
    (SELECT MAX(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM exploded s
ORDER BY customer_id DESC;
