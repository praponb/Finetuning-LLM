WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY burst_id DESC, id) = 1
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM deduped
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Ranking and gap features
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    session_start_flag,
    burst_id,
    prev_paid_at,
    new_group_flag,
    session_id,
    AVG(new_group_flag) OVER (PARTITION BY order_id, method ORDER BY CAST(prev_paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS new_group_flag_avg_30d,
    SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_new_group_flag,
    LEAD(new_group_flag, 1) OVER (PARTITION BY status ORDER BY amount, id) AS next_new_group_flag,
    RANK() OVER (ORDER BY amount NULLS FIRST) AS amount_rank
FROM islands_payments;
