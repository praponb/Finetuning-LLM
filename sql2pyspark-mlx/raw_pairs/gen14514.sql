WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN method IN ('bank_transfer', 'card') THEN 'Group A' ELSE 'Other' END AS method_group,
        ROUND(amount * 0.9, 2) AS adjusted_amount
    FROM payments
    WHERE method IS NOT NULL
      AND paid_at > DATE '2023-09-07'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        paid_at,
        SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM islands
),
with_gaps_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_paid_at) OVER (PARTITION BY order_id ORDER BY prev_paid_at, id) IS NULL
                 OR prev_paid_at > LAG(prev_paid_at) OVER (PARTITION BY order_id ORDER BY prev_paid_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions_payments
),
sessions_payments_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY prev_paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM flagged
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.prev_paid_at,
        src.new_group_flag,
        src.session_no,
        d.discount,
        d.status
    FROM sessions_payments_2 src
    INNER JOIN orders d
        ON src.order_id = d.id
)
-- Join a dimension and aggregate
SELECT
    dim.status AS order_status,
    dim.channel,
    s.status AS payment_status,
    COUNT(*) AS rows_total,
    ROUND(SUM(s.session_no), 2) AS total_session_no
FROM with_dims s
LEFT JOIN orders dim ON s.order_id = dim.id
GROUP BY dim.status, dim.channel, s.status;
