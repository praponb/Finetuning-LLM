WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        COALESCE(segment, 'OTHER') AS segment_clean
    FROM customers
    WHERE signup_date > DATE '2023-08-23'
      AND city IS NOT NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.start_date <= s.signup_date
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.order_date < s.signup_date
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(age) OVER (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_age,
        COUNT(age) OVER (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_age,
        MIN(age) OVER (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_age
    FROM kept
    WHERE segment <> 'Corporate'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.status = 'PAUSED'
        )
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            country,
            city,
            segment,
            age
        FROM eligible
    ) src
    PIVOT (
        AVG(age)
        FOR segment IN ('Consumer', 'Corporate')
    )
)
SELECT
    country,
    city,
    Consumer,
    Corporate
FROM by_type
