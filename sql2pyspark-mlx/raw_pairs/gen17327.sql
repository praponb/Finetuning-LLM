WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date_2
    FROM customers
    WHERE signup_date >= TO_DATE('2023-06-08')
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at >= s.signup_date
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY days_since_signup_date DESC, id) = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN days_since_signup_date_2 >= 76 THEN 'High' WHEN days_since_signup_date_2 >= 23 THEN 'Medium' ELSE 'Low' END AS days_since_signup_date_2_tier,
        CONCAT(segment, ' - ', country) AS segment_country
    FROM ranked
    WHERE city IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(days_since_signup_date) OVER (PARTITION BY city ORDER BY signup_date, id) AS prev_days_since_signup_date,
        LEAD(credit_limit) OVER (PARTITION BY city ORDER BY signup_date, id) AS next_credit_limit,
        LAG(credit_limit) OVER (PARTITION BY city ORDER BY signup_date, id) AS prev_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY days_since_signup_date, id) AS seq_no_2
    FROM cleaned
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.monthly_fee,
        x.subscription_end_date
    FROM metrics s
    JOIN LATERAL (
            SELECT c.monthly_fee, c.end_date AS subscription_end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date >= s.signup_date
        )
)
-- Multi-level aggregation
SELECT
    country,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count
FROM kept_customers
GROUP BY country
ORDER BY tokyo_count DESC;
