WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        HOUR(paid_at) AS paid_at_hour,
        TO_DATE(paid_at) AS paid_at_date,
        COALESCE(method, 'N/A') AS method_filled
    FROM payments
    WHERE method = 'promptpay'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        paid_at_hour,
        paid_at_date,
        method_filled,
        NTILE(4) OVER w_running AS paid_at_hour_bucket,
        DENSE_RANK() OVER (ORDER BY amount DESC) AS amount_dense_rank
    FROM base
    WINDOW w_running AS (ORDER BY paid_at_hour DESC, id)
),
base_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(status, '/', method) AS status_method
    FROM enriched
    WHERE paid_at >= DATE '2024-03-05'
      AND method IS NOT NULL
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM base_payments
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_paid_at) OVER (PARTITION BY method ORDER BY prev_paid_at, id) IS NULL
                 OR prev_paid_at > LAG(prev_paid_at) OVER (PARTITION BY method ORDER BY prev_paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM islands
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY prev_paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst_2
    FROM islands_payments
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged_2
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            method,
            status,
            paid_at_hour_bucket
        FROM sessions_payments
    ) src
    PIVOT (
        SUM(paid_at_hour_bucket) AS total,
        COUNT(1) AS n
        FOR status IN ('SUCCESS' AS success, 'REFUNDED' AS refunded)
    )
)
SELECT
    method,
    success_total,
    success_n,
    refunded_total,
    refunded_n,
    CASE WHEN method RLIKE '^(bank_transfer|cash)$' THEN 'EXTENDED' WHEN method RLIKE '^(card|paypal)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS method_zone
FROM pivoted
ORDER BY refunded_n DESC
