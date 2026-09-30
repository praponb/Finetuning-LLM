WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    CASE WHEN burst_id >= 7 THEN 'HIGH' ELSE 'LOW' END AS burst_id_tier,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(*) AS cnt,
    COUNT_IF(method = 'bank_transfer') AS bank_transfer_events
FROM islands
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', paid_at), CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN burst_id >= 7 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('MONTH', paid_at), CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END), ());
