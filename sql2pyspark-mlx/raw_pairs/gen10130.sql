WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND MONTH(c.end_date) IN (5, 7, 11)
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share,
        COUNT(credit_limit) OVER (PARTITION BY segment) AS segment_count_credit_limit
    FROM not_flagged
    WHERE city != 'London'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date >= s.signup_date
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT segment, id
    FROM kept
    EXCEPT ALL
    SELECT segment, id
    FROM customers
    WHERE is_active
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN kept_keys k ON s.segment = k.segment AND s.id = k.id
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    is_active,
    credit_limit,
    age_share
FROM kept_rows
ORDER BY country
