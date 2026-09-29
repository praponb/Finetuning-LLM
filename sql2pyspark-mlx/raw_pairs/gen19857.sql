WITH with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.shipping_fee,
        latest.order_discount
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.shipping_fee, c.discount AS order_discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date <= s.signup_date
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at <= s.signup_date
        )
),
kept_customers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at < s.signup_date
        )
)
SELECT
    id,
    name,
    country,
    city,
    segment,
    signup_date,
    credit_limit,
    age,
    shipping_fee
FROM kept_customers
ORDER BY shipping_fee;
