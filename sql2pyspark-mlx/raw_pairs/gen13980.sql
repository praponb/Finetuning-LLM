WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN status IN ('SUCCESS', 'REFUNDED') THEN 'Core' ELSE 'Other' END AS status_group
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-11-05 17:00:00'
      AND method IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session_2
    FROM islands
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM islands_payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        status_group,
        is_new_session,
        SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        method,
        amount,
        status,
        status_group,
        is_new_session,
        session_no,
        AVG(amount) OVER w_part AS running_avg_amount,
        MIN(amount) OVER w_part AS running_min_amount,
        MAX(amount) OVER w_part AS running_max_amount,
        COUNT(amount) OVER w_part AS running_count_amount,
        AVG(amount) OVER w_ordered AS method_avg_amount
    FROM sessionized
    WINDOW w_part AS (PARTITION BY status ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY method)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY session_no, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    amount,
    status,
    session_no,
    running_avg_amount,
    running_min_amount,
    running_max_amount,
    running_count_amount,
    rank_in_group
FROM latest
ORDER BY running_count_amount DESC NULLS LAST
