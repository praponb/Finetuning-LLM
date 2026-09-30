WITH valid AS (
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        CONCAT(name, ' - ', email) AS name_email,
        CONCAT(segment, ', ', email) AS segment_email
    FROM customers
    WHERE signup_date > DATE '2023-05-01'
      AND name NOT LIKE 'FR%'
),
enriched AS (
    SELECT
        id,
        name,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        segment_email,
        SUM(age) OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY city), 4) AS credit_limit_share,
        RANK() OVER (PARTITION BY segment ORDER BY age DESC NULLS FIRST) AS age_rank
    FROM valid
    WHERE age BETWEEN 35 AND 64
),
enriched_customers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS event_seq,
        NTILE(3) OVER (PARTITION BY city ORDER BY credit_limit, id) AS credit_limit_bucket
    FROM enriched
),
enriched_customers_2 AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY YEAR(signup_date), country ORDER BY credit_limit_bucket NULLS LAST) AS credit_limit_bucket_percent_rank,
        ROUND(credit_limit_bucket / SUM(credit_limit_bucket) OVER (PARTITION BY country), 4) AS credit_limit_bucket_share,
        LAG(credit_limit_share, 1) OVER (PARTITION BY country ORDER BY credit_limit_share, id) AS prev_credit_limit_share
    FROM enriched_customers
),
metrics AS (
    SELECT
        *,
        COUNT(event_seq) OVER w_part AS running_count_event_seq,
        SUM(event_seq) OVER w_part AS running_sum_event_seq,
        MAX(event_seq) OVER w_part AS running_max_event_seq,
        LEAD(credit_limit_share, 2) OVER w_recent AS next_credit_limit_share
    FROM enriched_customers_2
    WINDOW w_part AS (PARTITION BY city, country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY country ORDER BY credit_limit_share, id)
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.end_date < s.signup_date
        )
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY credit_limit_bucket, id) AS event_seq_2,
        LEAD(running_sum_event_seq, 1) OVER w_hist AS next_running_sum_event_seq
    FROM not_flagged
    WINDOW w_hist AS (PARTITION BY country ORDER BY credit_limit_bucket_share, id)
)
SELECT
    id,
    name,
    country,
    city,
    signup_date,
    segment_email,
    running_age,
    credit_limit_share,
    age_rank,
    CASE WHEN city RLIKE '^(Berlin|London)$' THEN 'EXTENDED' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS city_zone
FROM scored;
