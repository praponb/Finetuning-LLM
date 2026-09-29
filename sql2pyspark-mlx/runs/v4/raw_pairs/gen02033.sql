WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY method, order_id ORDER BY is_new_burst, id) AS is_new_burst_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY session_no DESC, id) AS event_seq,
        RANK() OVER (PARTITION BY status ORDER BY is_new_burst NULLS FIRST) AS is_new_burst_rank,
        ROW_NUMBER() OVER (ORDER BY paid_at, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY paid_at DESC, id) AS row_seq_2
    FROM islands
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY is_new_burst_rank DESC NULLS FIRST, id) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        is_new_burst,
        session_no,
        is_new_burst_bucket,
        event_seq,
        is_new_burst_rank,
        row_seq,
        row_seq_2,
        row_num,
        AVG(row_seq) OVER (ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS row_seq_avg_3_prior,
        SUM(row_seq) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        MAX(is_new_burst) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_new_burst_max_7d
    FROM ranked
)
SELECT
    id,
    amount,
    paid_at,
    status,
    is_new_burst,
    session_no,
    is_new_burst_rank,
    row_seq,
    row_seq_2,
    CASE WHEN status RLIKE '^(REFUNDED|FAILED)$' THEN 'EXTENDED' WHEN status RLIKE '^(SUCCESS)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS status_zone
FROM with_windows
ORDER BY is_new_burst_rank NULLS FIRST;
