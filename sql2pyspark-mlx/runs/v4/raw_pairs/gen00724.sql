WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Grouped metrics
SELECT
    status,
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    COUNT(*) AS event_count,
    COUNT_IF(method = 'cash') AS cash_events,
    APPROX_PERCENTILE(is_new_session, ARRAY(0.25, 0.5, 0.75)) AS is_new_session_quartiles,
    SUM(amount) FILTER (WHERE status = 'REFUNDED') AS refunded_total_amount,
    MAX(is_new_session) FILTER (WHERE status = 'REFUNDED') AS refunded_max_is_new_session,
    GROUPING(status) AS grouping_level
FROM sessionized
GROUP BY GROUPING SETS ((status, DATE_TRUNC('YEAR', paid_at)), (status))
ORDER BY refunded_total_amount DESC NULLS LAST
