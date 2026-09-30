WITH latest AS (
    SELECT
        id,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY age DESC NULLS LAST) AS dedup_rank
        FROM customers
    ) x
    WHERE dedup_rank = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY age DESC NULLS LAST) AS rn
        FROM latest
    ) t
    WHERE rn = 1
),
with_last AS (
    SELECT
        s.id,
        s.country,
        s.is_active,
        s.credit_limit,
        s.age,
        s.rn,
        latest.end_date
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    SELECT
        id,
        credit_limit,
        age,
        LAG(age, 2) OVER (PARTITION BY country ORDER BY credit_limit, id) AS prev_age,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit
    FROM with_last
),
with_last_customers AS (
    SELECT
        s.id,
        s.prev_age,
        s.first_known_credit_limit,
        latest.start_date,
        latest.plan
    FROM metrics s
    JOIN LATERAL (
            SELECT c.start_date, c.plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_customers_2 AS (
    SELECT
        s.id,
        s.prev_age,
        latest.shipping_fee
    FROM with_last_customers s
    LEFT JOIN LATERAL (
            SELECT c.shipping_fee
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last_customers_2 s
    WHERE EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.status NOT IN ('OPEN', 'FROZEN')
        )
)
SELECT id, shipping_fee
FROM not_flagged
ORDER BY id DESC NULLS LAST
