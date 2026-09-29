WITH latest_match AS (
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
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.browser = 'Chrome'
        )
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        shipping_fee,
        order_discount,
        DATE_SUB(signup_date, 86) AS signup_date_minus_86d
    FROM eligible
    WHERE city IS NOT DISTINCT FROM 'New York'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        signup_date,
        is_active,
        shipping_fee,
        order_discount,
        signup_date_minus_86d,
        LEAD(age, 1) OVER (PARTITION BY DATE_TRUNC('YEAR', signup_date_minus_86d), segment ORDER BY credit_limit, id) AS next_age,
        AVG(order_discount) OVER w_ordered AS order_discount_avg_6
    FROM valid
    WHERE segment NOT IN ('Corporate', 'SMB')
    WINDOW w_ordered AS (PARTITION BY MONTH(signup_date), segment ORDER BY signup_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
eligible_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.shipping_fee < 14
        )
)
SELECT
    id,
    email,
    signup_date,
    shipping_fee,
    order_discount,
    signup_date_minus_86d,
    next_age,
    order_discount_avg_6
FROM eligible_customers;
