WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(method, ', ', status) AS method_status,
        LENGTH(status) AS status_length
    FROM payments
    WHERE status IN ('FAILED', 'SUCCESS', 'REFUNDED')
      AND paid_at >= TIMESTAMP '2023-05-14 10:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(status_length) OVER (PARTITION BY order_id, method ORDER BY amount, id) AS prev_status_length,
        LEAD(status_length) OVER (PARTITION BY order_id, method ORDER BY amount, id) AS next_status_length,
        LEAD(amount) OVER (PARTITION BY order_id, method ORDER BY amount, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY amount, id) AS seq_no
    FROM valid
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        method_status,
        status_length,
        next_status_length,
        next_amount,
        seq_no,
        seq_no + next_amount AS seq_no_plus_next_amount,
        CONCAT_WS(',', method, status) AS method_status_key,
        COALESCE(method, 'NONE') AS method_filled
    FROM with_windows
    WHERE paid_at > TO_DATE('2023-11-05')
      AND seq_no BETWEEN 4 AND 6
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY seq_no, id) = 1
),
cleaned_payments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        paid_at,
        status,
        status_length,
        next_status_length,
        next_amount,
        seq_no_plus_next_amount,
        LEAST(seq_no_plus_next_amount, status_length) AS least_seq_no_plus_next_amount_status_length,
        GREATEST(next_status_length, amount) AS greatest_next_status_length_amount,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM deduped
    WHERE paid_at >= TIMESTAMP '2023-05-31 15:00:00'
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN status_length >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_length_tier,
        DATE_TRUNC('WEEK', paid_at) AS week_start,
        APPROX_PERCENTILE(next_amount, ARRAY(0.25, 0.5, 0.75)) AS next_amount_quartiles,
        SIZE(COLLECT_SET(status)) AS num_status,
        MIN(paid_at) AS first_seen,
        SUM(CASE WHEN status = 'FAILED' THEN next_status_length ELSE 0 END) AS failed_next_status_length
    FROM cleaned_payments
    GROUP BY status_length_tier, week_start
)
SELECT
    status_length_tier,
    week_start,
    next_amount_quartiles,
    num_status,
    first_seen,
    failed_next_status_length
FROM stats
ORDER BY failed_next_status_length DESC NULLS LAST;
