WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.browser,
        src.started_at,
        src.country,
        c.signup_date,
        c.credit_limit
    FROM sessions src
    INNER JOIN customers c
        ON src.customer_id = c.id
    WHERE c.signup_date > '2023-04-21'
),
with_dims_sessions AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        c.name,
        c.credit_limit AS customer_credit_limit,
        c.country AS customer_country
    FROM with_dims s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(customer_credit_limit) OVER (PARTITION BY customer_id) AS customer_id_sum_customer_credit_limit,
        NTILE(3) OVER w_part AS customer_credit_limit_bucket,
        NTILE(4) OVER (ORDER BY customer_credit_limit DESC, id) AS customer_credit_limit_bucket_2
    FROM with_dims_sessions
    WINDOW w_part AS (PARTITION BY customer_country ORDER BY customer_credit_limit, id)
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_credit_limit,
        s.customer_country,
        s.customer_id_sum_customer_credit_limit,
        s.customer_credit_limit_bucket,
        x.event_time,
        x.event_type
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.event_time, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    customer_credit_limit,
    customer_country,
    customer_credit_limit_bucket,
    event_time,
    event_type
FROM latest_match;
