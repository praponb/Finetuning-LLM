WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessionized
),
sessionized_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER (ORDER BY session_id_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        PERCENT_RANK() OVER (ORDER BY session_id_2) AS session_id_2_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) AS seq_no,
        AVG(new_group_flag) OVER (PARTITION BY method) AS method_avg_new_group_flag,
        SUM(session_id) OVER (PARTITION BY status) AS status_sum_session_id
    FROM sessionized_payments
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        amount,
        status,
        prev_paid_at,
        new_group_flag,
        session_id_2,
        LAG(seq_no) OVER w_ordered AS prev_seq_no,
        LEAD(seq_no) OVER w_ordered AS next_seq_no,
        LEAD(session_id_2) OVER w_ordered AS next_session_id_2,
        LAG(session_id_2) OVER w_ordered AS prev_session_id_2,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY session_id_2_percent_rank, id)
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_paid_at) OVER (PARTITION BY status ORDER BY prev_paid_at, id) IS NULL
                 OR prev_paid_at > LAG(prev_paid_at) OVER (PARTITION BY status ORDER BY prev_paid_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY prev_paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged_2
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(prev_paid_at) OVER (PARTITION BY status ORDER BY prev_paid_at, id) AS prev_prev_paid_at
    FROM sessions_payments
),
with_gaps_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_prev_paid_at IS NULL OR prev_paid_at > prev_prev_paid_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag_2
    FROM prev_events
),
sessionized_payments_2 AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        status,
        prev_paid_at,
        new_group_flag,
        prev_seq_no,
        next_seq_no,
        next_session_id_2,
        row_seq,
        is_new_session,
        island_id,
        SUM(new_group_flag_2) OVER (PARTITION BY status ORDER BY prev_paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
)
SELECT
    id,
    prev_paid_at,
    new_group_flag,
    prev_seq_no,
    next_seq_no,
    next_session_id_2,
    row_seq,
    is_new_session,
    island_id
FROM sessionized_payments_2
ORDER BY next_seq_no DESC;
