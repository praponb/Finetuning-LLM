WITH filtered AS (
    SELECT
        *,
        TRIM(name) AS clean_name,
        COALESCE(segment, 'UNKNOWN') AS segment_filled
    FROM customers
    WHERE signup_date > DATE '2023-03-10'
),
with_windows AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        is_active,
        credit_limit,
        clean_name,
        segment_filled,
        LEAD(credit_limit, 1) OVER (PARTITION BY country ORDER BY signup_date, id) AS next_credit_limit,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('month', signup_date), city ORDER BY credit_limit, id) AS credit_limit_bucket
    FROM filtered
),
with_latest AS (
    SELECT
        s.id,
        s.email,
        s.segment,
        s.is_active,
        s.credit_limit,
        s.clean_name,
        s.segment_filled,
        s.credit_limit_bucket,
        x.start_date
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        id,
        email,
        segment,
        credit_limit,
        clean_name,
        segment_filled,
        credit_limit_bucket,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY start_date DESC, id) AS seq_no,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY YEAR(start_date), segment), 4) AS credit_limit_share
    FROM with_latest
    WHERE NOT is_active
)
SELECT
    id,
    email,
    segment_filled,
    credit_limit_bucket,
    seq_no,
    last_known_credit_limit,
    credit_limit_share
FROM metrics;
