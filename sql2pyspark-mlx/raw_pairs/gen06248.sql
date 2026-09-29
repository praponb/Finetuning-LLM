WITH with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.status,
        latest.order_discount
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.status, c.discount AS order_discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.end_date < s.signup_date
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY age NULLS LAST, id) AS rank_in_group
        FROM not_flagged
    ) x
    WHERE rank_in_group = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT city, segment
    FROM latest
    EXCEPT DISTINCT
    SELECT city, segment
    FROM customers
    WHERE segment IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.city = k.city AND s.segment = k.segment
),
scored AS (
    -- Ranking and gap features
    SELECT
        name,
        email,
        is_active,
        credit_limit,
        age,
        status,
        order_discount,
        rank_in_group,
        CORR(age, credit_limit) OVER (PARTITION BY city) corr_age_credit_limit
    FROM kept_rows
)
SELECT
    name,
    email,
    is_active,
    order_discount,
    rank_in_group,
    corr_age_credit_limit
FROM scored
