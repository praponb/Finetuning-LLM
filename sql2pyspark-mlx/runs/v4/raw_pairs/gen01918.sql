WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END AS is_failed,
        HOUR(paid_at) AS paid_at_hour,
        COALESCE(method, 'UNKNOWN') AS method_filled
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2023-10-01 18:00:00')
      AND order_id IS NOT NULL
      AND method = 'cash'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(paid_at_hour) OVER (PARTITION BY method) AS method_max_paid_at_hour,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) AS seq_no,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY paid_at_hour DESC, id) AS paid_at_hour_bucket
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    order_id,
    method,
    paid_at,
    is_failed,
    seq_no,
    session_start_flag,
    session_id,
    CASE WHEN method RLIKE '^(bank_transfer|promptpay)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS method_zone
FROM islands
