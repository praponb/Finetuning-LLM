WITH scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY YEAR(signup_date), country ORDER BY signup_date, id) AS seq_no
    FROM customers
),
cleaned AS (
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
        age,
        seq_no,
        seq_no + row_seq AS seq_no_plus_row_seq,
        COALESCE(city, 'NONE') AS city_clean
    FROM scored
    WHERE signup_date >= TO_DATE('2023-02-24')
),
eligible AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at < s.signup_date
        )
),
latest_match AS (
    SELECT s.*, x.order_status
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at >= '2023-04-01'
        )
),
not_flagged_customers AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date <= s.signup_date
        )
)
SELECT
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    ROUND(AVG(seq_no_plus_row_seq), 2) AS avg_seq_no_plus_row_seq,
    COUNT(*) AS event_count,
    SUM(credit_limit) AS total_credit_limit,
    COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count,
    APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    GROUPING_ID() AS grouping_level
FROM not_flagged_customers
GROUP BY CUBE (DATE_TRUNC('WEEK', signup_date))
HAVING ROUND(AVG(seq_no_plus_row_seq), 2) > 0
