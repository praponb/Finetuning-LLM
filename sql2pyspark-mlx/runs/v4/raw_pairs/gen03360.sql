WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        paid_at,
        status,
        DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
    FROM payments
    WHERE paid_at >= DATE '2023-07-11'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY days_since_paid_at DESC NULLS LAST, id ASC) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER w_recent AS days_since_paid_at_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id ASC) AS row_seq,
        AVG(days_since_paid_at) OVER w_running AS days_since_paid_at_avg_2,
        FIRST_VALUE(days_since_paid_at, TRUE) OVER w_ordered AS first_known_days_since_paid_at,
        LAG(days_since_paid_at, 2, 0) OVER (ORDER BY paid_at ASC, id ASC) AS prev_days_since_paid_at
    FROM latest
    WINDOW w_recent AS (PARTITION BY order_id, status ORDER BY days_since_paid_at ASC, id ASC), w_running AS (ORDER BY days_since_paid_at ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        paid_at,
        status,
        days_since_paid_at,
        days_since_paid_at_bucket,
        row_seq,
        days_since_paid_at_avg_2,
        first_known_days_since_paid_at,
        prev_days_since_paid_at,
        LAG(days_since_paid_at_bucket, 2) OVER w_grp AS prev_days_since_paid_at_bucket,
        AVG(first_known_days_since_paid_at) OVER w_recent AS order_id_avg_first_known_days_since_paid_at
    FROM metrics
    WINDOW w_grp AS (PARTITION BY order_id, status ORDER BY days_since_paid_at_avg_2 ASC, id ASC), w_recent AS (PARTITION BY order_id)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, prev_days_since_paid_at - days_since_paid_at AS prev_days_since_paid_at_minus_days_since_paid_at
    FROM metrics_payments
    WHERE order_id IS NOT NULL
      AND first_known_days_since_paid_at <= 66
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        days_since_paid_at_bucket
    FROM valid
) src
PIVOT (
    SUM(days_since_paid_at_bucket) AS total,
    COUNT(1) AS cnt
    FOR status IN ('FAILED', 'REFUNDED')
)
