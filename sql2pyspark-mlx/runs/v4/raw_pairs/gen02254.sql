WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS rank_in_group
        FROM customers
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
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
        age,
        rank_in_group,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment, country), 4) AS credit_limit_share,
        AVG(age) OVER (PARTITION BY city, country ORDER BY signup_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS age_avg_3,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country), 4) AS credit_limit_share_2
    FROM latest
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        city,
        segment,
        is_active,
        credit_limit,
        rank_in_group,
        age_avg_3,
        credit_limit_share_2,
        COALESCE(credit_limit, 0) AS credit_limit_filled,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM with_windows
    WHERE city = 'Chiang Mai'
),
latest_customers AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age_avg_3 DESC, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.discount
    FROM latest_customers s
    LEFT JOIN LATERAL (
            SELECT c.discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.plan = 'basic'
        )
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.shipping_fee,
        latest.order_amount
    FROM kept s
    JOIN LATERAL (
            SELECT c.shipping_fee, c.amount AS order_amount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    segment,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    ROUND(AVG(age_avg_3), 2) AS avg_age_avg_3,
    APPROX_PERCENTILE(credit_limit, 0.9) AS credit_limit_p90,
    MAX(credit_limit) AS max_credit_limit,
    MIN(credit_limit_share_2) AS min_credit_limit_share_2,
    COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count
FROM with_last
GROUP BY 1
