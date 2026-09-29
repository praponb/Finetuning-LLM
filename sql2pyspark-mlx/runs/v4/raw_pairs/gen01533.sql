WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TO_DATE(paid_at) AS paid_at_date,
        COALESCE(method, 'OTHER') AS method_clean
    FROM payments
    WHERE method IS NOT NULL
      AND order_id IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    status,
    ROUND(AVG(session_id), 2) AS avg_session_id,
    MAX(paid_at) AS last_seen,
    STDDEV_SAMP(amount) AS amount_stddev,
    COUNT_IF(status = 'FAILED') AS failed_events
FROM sessions_payments
GROUP BY 1
