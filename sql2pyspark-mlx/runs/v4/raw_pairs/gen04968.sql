WITH enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.channel,
        b.amount AS order_amount,
        b.shipping_fee
    FROM payments src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
enriched_payments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER w_grp AS order_amount_bucket,
        SUM(order_amount) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_order_amount
    FROM enriched
    WINDOW w_grp AS (PARTITION BY order_id, method ORDER BY order_amount, id)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM enriched_payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        channel,
        order_amount,
        shipping_fee,
        order_amount_bucket,
        running_order_amount,
        is_new_session,
        session_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_amount, id) AS event_seq,
        AVG(order_amount_bucket) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS order_amount_bucket_avg_6_prior,
        LAST_VALUE(running_order_amount, TRUE) OVER (PARTITION BY order_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_order_amount,
        AVG(running_order_amount) OVER w_recent AS running_order_amount_avg_3,
        FIRST_VALUE(order_amount) IGNORE NULLS OVER (PARTITION BY status, channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_order_amount
    FROM sessionized
    WINDOW w_recent AS (ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session_2
    FROM scored
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY channel ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
        COUNT(*) AS event_count,
        COUNT(DISTINCT method) AS distinct_methods
    FROM sessions_payments
    GROUP BY quarter_start
)
SELECT
    quarter_start,
    event_count,
    distinct_methods
FROM summary
ORDER BY quarter_start DESC, distinct_methods
LIMIT 20;
