WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        paid_at,
        status,
        COALESCE(amount, 0) AS amount_filled,
        ROUND(amount * 100, 2) AS adjusted_amount
    FROM payments
    WHERE order_id IS NOT NULL
      AND paid_at >= TIMESTAMP '2023-08-27 15:00:00'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        paid_at,
        status,
        amount_filled,
        adjusted_amount,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM valid
    WHERE adjusted_amount > 43
      AND method IS DISTINCT FROM 'cash'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM base
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        paid_at,
        status,
        amount_filled,
        adjusted_amount,
        status_prefix,
        prev_paid_at,
        FIRST_VALUE(amount_filled, TRUE) OVER (PARTITION BY DATE_TRUNC('WEEK', prev_paid_at), status ORDER BY prev_paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount_filled,
        COUNT(session_start_flag) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_paid_at), method) AS prev_paid_at_period_count_session_start_flag,
        FIRST_VALUE(session_start_flag, TRUE) OVER (PARTITION BY status, method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_start_flag
    FROM sessionized
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(amount_filled, 2) OVER (PARTITION BY status ORDER BY adjusted_amount, id) AS next_amount_filled,
        AVG(adjusted_amount) OVER (PARTITION BY status ORDER BY prev_paid_at_period_count_session_start_flag, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS adjusted_amount_avg_3_prior,
        MAX(amount_filled) OVER (PARTITION BY DATE_TRUNC('YEAR', paid_at), status ORDER BY CAST(prev_paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_filled_max_7d,
        SUM(amount_filled) OVER (PARTITION BY TO_DATE(prev_paid_at), status ORDER BY first_known_session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_filled,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY prev_paid_at_period_count_session_start_flag DESC, id) AS seq_no
    FROM with_windows
    WHERE status = 'SUCCESS'
)
-- Multi-level aggregation
SELECT
    seq_no,
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    CASE WHEN first_known_amount_filled >= 62 THEN 'LARGE' ELSE 'SMALL' END AS first_known_amount_filled_tier,
    ROUND(SUM(first_known_session_start_flag) / NULLIF(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END), 0), 2) AS first_known_session_start_flag_per_success,
    COUNT_IF(status = 'REFUNDED') AS refunded_events,
    COUNT_IF(status = 'FAILED') AS failed_events
FROM enriched
GROUP BY GROUPING SETS ((seq_no, DATE_TRUNC('QUARTER', paid_at), CASE WHEN first_known_amount_filled >= 62 THEN 'LARGE' ELSE 'SMALL' END), (seq_no, DATE_TRUNC('QUARTER', paid_at)), (seq_no), ())
ORDER BY seq_no DESC NULLS LAST
