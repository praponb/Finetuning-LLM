WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(method, 'N/A') AS method_filled,
        COALESCE(method, 'N/A') AS method_filled_2
    FROM payments
    WHERE paid_at > TO_DATE('2023-06-18')
      AND method IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        status,
        method_filled,
        method_filled_2,
        DATE_TRUNC('month', paid_at) AS paid_at_period
    FROM base
    WHERE order_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT *, LAST_VALUE(method, TRUE) OVER (ORDER BY paid_at_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method
    FROM filtered
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at_period) OVER (PARTITION BY method ORDER BY paid_at_period, id) AS prev_paid_at_period
    FROM metrics
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at_period IS NULL OR paid_at_period > prev_paid_at_period + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    id,
    method,
    method_filled,
    method_filled_2,
    last_known_method,
    is_new_burst,
    session_no
FROM islands
ORDER BY id DESC NULLS FIRST, is_new_burst;
