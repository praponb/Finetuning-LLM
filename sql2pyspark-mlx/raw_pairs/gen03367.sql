WITH with_prev AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
flagged AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_sessions AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    customer_id,
    CASE WHEN burst_id >= 9 THEN 'HIGH' ELSE 'LOW' END AS burst_id_tier,
    COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_rows,
    ROUND(AVG(burst_id), 2) AS avg_burst_id
FROM sessions_sessions
GROUP BY DATE_TRUNC('QUARTER', started_at), customer_id, CASE WHEN burst_id >= 9 THEN 'HIGH' ELSE 'LOW' END
ORDER BY customer_id NULLS LAST
