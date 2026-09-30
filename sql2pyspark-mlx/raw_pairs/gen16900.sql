WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        CONCAT(method, '/', status) AS method_status,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-09-13 00:00:00'
      AND order_id = 31
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at
    FROM base
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('WEEK', prev_paid_at) AS prev_paid_at_period
    FROM islands
    WHERE status IN ('REFUNDED', 'FAILED', 'SUCCESS')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(burst_id) OVER w_part AS next_burst_id,
        LAG(burst_id) OVER w_part AS prev_burst_id,
        LEAD(is_new_session) OVER w_part AS next_is_new_session,
        LAG(is_new_session) OVER w_part AS prev_is_new_session,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM valid
    WINDOW w_part AS (PARTITION BY method ORDER BY is_new_session, id)
),
metrics_payments AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(prev_is_new_session) IGNORE NULLS OVER w_recent AS last_known_prev_is_new_session,
        FIRST_VALUE(is_new_session, TRUE) OVER (PARTITION BY method ORDER BY prev_is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_new_session,
        AVG(prev_burst_id) OVER (ORDER BY prev_is_new_session, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prev_burst_id_avg_3,
        MAX(burst_id) OVER (PARTITION BY TO_DATE(prev_paid_at), method) AS prev_paid_at_to_date_max_burst_id
    FROM metrics
    WINDOW w_recent AS (PARTITION BY method ORDER BY prev_burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Running and moving aggregates
SELECT
    id,
    method,
    paid_at,
    method_clean,
    prev_paid_at,
    next_burst_id,
    prev_is_new_session,
    first_known_is_new_session,
    prev_burst_id_avg_3,
    LEAD(prev_is_new_session) OVER w_running AS next_prev_is_new_session,
    LAG(prev_burst_id) OVER w_running AS prev_prev_burst_id,
    LEAD(prev_burst_id) OVER w_running AS next_prev_burst_id,
    ROW_NUMBER() OVER w_running AS row_seq,
    LAST_VALUE(prev_is_new_session) IGNORE NULLS OVER (PARTITION BY TO_DATE(prev_paid_at), status ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_is_new_session_2
FROM metrics_payments
WINDOW w_running AS (PARTITION BY status ORDER BY prev_paid_at_period, id);
